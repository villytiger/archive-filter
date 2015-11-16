import std.algorithm.mutation: strip, stripLeft, stripRight;
import std.algorithm.sorting: multiSort;
import std.bitmanip: littleEndianToNative, nativeToLittleEndian;
import std.functional: binaryReverseArgs;
import std.getopt;
import std.path: absolutePath, asNormalizedPath, baseName, buildNormalizedPath, chainPath,
        globMatch, pathSplitter;
import std.range: dropBack;
import std.range.primitives;
import std.stdio: stderr;

import vibe.d;
import vibe.stream.stdio: StderrStream, StdinStream, StdoutStream;

static immutable PKZIP_LOCAL_FILE_HEADER_MAGIC = [0x50, 0x4b ,0x03, 0x04];
static immutable PKZIP_CENTRAL_DIRECTORY_FILE_HEADER_MAGIC = [0x50, 0x4b ,0x01, 0x02];
static immutable PKZIP_END_OF_CENTRAL_DIRECTORY_RECORD_MAGIC = [0x50, 0x4b ,0x05, 0x06];

type get(type)(InputStream stream) {
	ubyte[type.sizeof] result;
	stream.read(result);
	return littleEndianToNative!type(result);
}

void skip(InputStream stream, size_t length) {
        auto s = cast(RandomAccessStream)stream;
	if (s) {
		s.seek(s.tell() + length);
	} else {
		ubyte[4096] b;
		while (length) {
			auto l = min(b.length, length);
			stream.read(b[0..l]);
			length -= l;
		}
	}
}

class UngetInputStream: InputStream {
private:
	InputStream mStream;
	ubyte[] mData;

public:
	this(InputStream stream) {
		mStream = stream;
	}

	override @property bool empty() { return mStream.empty(); }

	override @property ulong leastSize() { return mStream.leastSize(); }

	override @property bool dataAvailableForRead() { return mStream.dataAvailableForRead(); }

	override const(ubyte)[] peek() { return mStream.peek(); }

	override void read(ubyte[] dst) {
		if (!mData.empty) {
			if (mData.length <= dst.length) {
				size_t l = mData.length;
				dst[0..mData.length] = mData[];
				mData.length = 0;
				mStream.read(dst[l..$]);
			} else {
				dst = mData[0..dst.length];
				mData = mData[dst.length..$];
			}
		} else {
			mStream.read(dst);
		}
	}

	@property inout(InputStream) sourceStream() inout { return mStream; }

	void unget(ubyte[] data) {
		mData ~= data;
	}
}

interface ArchiveFilter {
        bool match(string path);
}

class PathFilter: ArchiveFilter {
private:
        string mPath;

public:
        this(string path) {
                mPath = path;
        }

        bool match(string path) {
                if (path.startsWith(mPath)) return true;
                else return false;
        }
}

class GlobFilter: ArchiveFilter {
private:
        string mPattern;

public:
        this(string pattern) {
                mPattern = pattern;
        }

        bool match(string path) {
                return globMatch(path, mPattern);
        }
}

class DirectoryFilter: ArchiveFilter {
private:
        string mPath;

public:
        this(string path) {
                mPath = path;
                if (!path.empty && !path.endsWith('/')) mPath ~= '/';
        }

        bool match(string path) {
                if (!path.startsWith(mPath)) return false;
                else if (path[mPath.length..$].stripRight('/').canFind('/')) return false;
                else if (path.length == mPath.length) return false;
                else return true;
        }
}

align(1) struct LocalFileHeader {
        enum MAGIC = 0x504B0304;
        uint signatue;
        ushort ver;
        ushort flags;
        ushort compression;
        ushort modificationTime;
        ushort modificationDate;
        uint crc32;
        uint compressedSize;
        uint uncompressedSize;
        ushort fileNameLength;
        ushort extraFieldLength;
}

ubyte[] byteBuffer(Type)(ref Type v) {
        return (cast(ubyte*)&v)[0..v.sizeof];
}

class ArchiveProcessor {
private:
	UngetInputStream mInput;
	OutputStream mOutput;
	ArchiveFilter mFilter;
        uint[string] mOffsets;

        void processLocalFiles(Delegate)(Delegate process) {
                while (!mInput.empty()) {
                        LocalFileHeader header = void;
                        mInput.read(header.byteBuffer[0..4]);
                        if (header.signature
                }
        }

                /*void processZip64EndOfCentralDirectoryRecord() {
                ubyte[56] header;
                mInput.read(header);
                if (header[0..4] != PKZIP_ZIP64__END_OF_CENTRAL_DIRECTORY_RECORD_MAGIC) {
                        mInput.unget(header[0..4]);
                        return;
                }

                header[24..32] = nativeToLittleEndian(totalEntries);
                header[32..40] = nativeToLittleEndian(totalEntries);
                header[40..48] = nativeToLittleEndian(centralDirectorySize);
                header[48..56] = nativeToLittleEndian(centralDirectoryOffset);

                mOutput.write(header);
        }

        void processZip64EndOfCentralDirectoryLocator() {
                ubyte[20] header;
                mInput.read(header);
                if (header[0..4] != PKZIP_ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_MAGIC) {
                        mInput.unget(header[0..4]);
                        return;
                }

                header[24..32] = nativeToLittleEndian(totalEntries);
                header[32..40] = nativeToLittleEndian(totalEntries);
                header[40..48] = nativeToLittleEndian(centralDirectorySize);
                header[48..56] = nativeToLittleEndian(centralDirectoryOffset);

                mOutput.write(header);
                mOutput.write(mInput);
                }*/

public:
	this(InputStream input, OutputStream output, ArchiveFilter filter) {
		mInput =  new UngetInputStream(input);
		mOutput = output;
		mFilter = filter;
	}

        void process() {
                void delegate() processFile;

                processLocalFiles(processFile);

		uint centralDirectoryOffset = 0;
		while (!mInput.empty()) {
			ubyte[30] header;
			mInput.read(header[0..PKZIP_LOCAL_FILE_HEADER_MAGIC.length]);
			if (header[0..PKZIP_LOCAL_FILE_HEADER_MAGIC.length] != PKZIP_LOCAL_FILE_HEADER_MAGIC) {
				mInput.unget(header[0..4]);
				break;
			} else {
				mInput.read(header[4..$]);
			}

			auto compressedSize = littleEndianToNative!uint(header[18..22]);
			auto fileNameLength = littleEndianToNative!ushort(header[26..28]);
			auto extraFieldLength = littleEndianToNative!ushort(header[28..30]);

			auto fileName = new char[fileNameLength];
			mInput.read(cast(ubyte[])fileName);

			if (!mFilter.match(fileName.to!string())) {
				mInput.sourceStream.skip(extraFieldLength);
				mInput.sourceStream.skip(compressedSize);
				continue;
			}

			mOffsets[fileName.to!string()] = centralDirectoryOffset;

			mOutput.write(header);
			mOutput.write(fileName);
			mOutput.write(mInput, extraFieldLength + compressedSize);

			centralDirectoryOffset += header.length + fileName.length
				+ extraFieldLength + compressedSize;
		}

		ushort totalEntries = 0;
		uint centralDirectorySize = 0;
		while (!mInput.empty()) {
			ubyte[46] header;
			mInput.read(header[0..PKZIP_CENTRAL_DIRECTORY_FILE_HEADER_MAGIC.length]);
                        if (header[0..PKZIP_CENTRAL_DIRECTORY_FILE_HEADER_MAGIC.length] != PKZIP_CENTRAL_DIRECTORY_FILE_HEADER_MAGIC) {
                                std.stdio.writefln("asd: %(0x%0.2x, %)", header[0..4]);
				mInput.unget(header[0..4]);
				break;
			} else {
				mInput.read(header[4..$]);
			}

			auto fileNameLength = littleEndianToNative!ushort(header[28..30]);
			auto extraFieldLength = littleEndianToNative!ushort(header[30..32]);
			auto fileCommentLength = littleEndianToNative!ushort(header[32..34]);

			auto fileName = new char[fileNameLength];
			mInput.read(cast(ubyte[])fileName);

			if (!mFilter.match(fileName.to!string())) {
				mInput.sourceStream.skip(extraFieldLength);
				mInput.sourceStream.skip(fileCommentLength);
				continue;
			}

			header[42..46] = nativeToLittleEndian(mOffsets[fileName]);

			mOutput.write(header);
			mOutput.write(fileName);
			mOutput.write(mInput, extraFieldLength + fileCommentLength);

			++totalEntries;
			centralDirectorySize += header.length + fileName.length
				+ extraFieldLength + fileCommentLength;
		}

		ubyte[22] header;
                mInput.read(header);
                std.stdio.writefln("0x%x", centralDirectoryOffset);
                std.stdio.writefln("0x%x", centralDirectorySize);
                std.stdio.writefln("%(0x%0.2x, %)", header);
		if (header[0..PKZIP_END_OF_CENTRAL_DIRECTORY_RECORD_MAGIC.length] != PKZIP_END_OF_CENTRAL_DIRECTORY_RECORD_MAGIC) {
			return;
			//TODO throw
		}

		header[8..10] = nativeToLittleEndian(totalEntries);
		header[10..12] = nativeToLittleEndian(totalEntries);
		header[12..16] = nativeToLittleEndian(centralDirectorySize);
		header[16..20] = nativeToLittleEndian(centralDirectoryOffset);

		mOutput.write(header);
		mOutput.write(mInput);
	}

	string[] list() {
		Appender!(string[]) result;

		while (!mInput.empty()) {
			ubyte[30] header;
			mInput.read(header[0..PKZIP_LOCAL_FILE_HEADER_MAGIC.length]);
			if (header[0..PKZIP_LOCAL_FILE_HEADER_MAGIC.length] != PKZIP_LOCAL_FILE_HEADER_MAGIC) {
				break;
			} else {
				mInput.read(header[4..$]);
			}

			auto compressedSize = littleEndianToNative!uint(header[18..22]);
			auto fileNameLength = littleEndianToNative!ushort(header[26..28]);
			auto extraFieldLength = littleEndianToNative!ushort(header[28..30]);

			auto fileNameBuf = new char[fileNameLength];
			mInput.read(cast(ubyte[])fileNameBuf);
			auto fileName = fileNameBuf.to!string();

			mInput.sourceStream.skip(extraFieldLength);
			mInput.sourceStream.skip(compressedSize);

			if (!mFilter.match(fileName)) continue;
			result.put(fileName);
		}

		return result.data;
	}
}

struct FileEntry {
        bool isDirectory;
        string name;
        ulong size;
        std.datetime.SysTime timeModified;
	string url;
        string downloadUrl;
}

auto splitPath(Path documentRoot, Path path) {
        Path localPath = path;
        Path absolutePath = documentRoot ~ localPath;
        absolutePath.endsWithSlash = false;

        while (!localPath.empty && !existsFile(absolutePath)) {
                localPath = localPath.parentPath;
                absolutePath = absolutePath.parentPath;
                absolutePath.endsWithSlash = false;
        }

        if (absolutePath.getFileInfo().isDirectory) localPath.endsWithSlash = true;

        auto internalPath = (documentRoot ~ path).relativeTo(absolutePath);

        return tuple!("local", "internal")(localPath, internalPath);
}

FileEntry[] sortFiles(FileEntry[] files, HTTPServerRequest req) {
        auto sortColumn = req.query.get("sort-column");
        if (sortColumn) {
                bool delegate(FileEntry, FileEntry) pred;
                switch (req.query.get("sort-column")) {
                default: pred = (f1, f2) => f1.name < f2.name; break;
                case "size": pred = (f1, f2) => f1.isDirectory != f2.isDirectory ? f1.isDirectory > f2.isDirectory : f1.size < f2.size; break;
                case "date": pred = (f1, f2) => f1.timeModified < f2.timeModified; break;
                }

                if (req.query.get("sort-order") == "descending") {
                        files.sort!(binaryReverseArgs!pred);
                } else {
                        files.sort!pred;
                }
        } else {
                files.multiSort!("a.isDirectory > b.isDirectory", "a.name < b.name");
        }

        return files;
}

string humanReadableInteger(T)(T i) {
        if (T r = i / 1000000000) return r.to!string ~ "G";
        if (T r = i / 1000000) return r.to!string ~ "M";
        if (T r = i / 1000) return r.to!string ~ "K";
        else return i.to!string;
}

void getArchive(string filePath, HTTPServerRequest req, HTTPServerResponse res) {
        string internalPath = req.query.get("path");
        string globPattern = req.query.get("glob-pattern");

        auto input = openFile(filePath);
        scope (exit) input.close();

        ArchiveFilter filter;
        if (globPattern) filter = new GlobFilter(globPattern);
        else filter= new PathFilter(internalPath);

        auto processor = new ArchiveProcessor(input, res.bodyWriter, filter);

        res.contentType = "application/octet";
        res.headers["Content-Disposition"] = "attachment; filename=" ~ filePath.baseName;
        processor.process();
}

void listArchive(string filePath, HTTPServerRequest req, HTTPServerResponse res) {
        string internalPath = req.query.get("path");

        auto input = openFile(filePath);
        scope (exit) input.close();

        auto filter = new DirectoryFilter(internalPath);
        auto processor = new ArchiveProcessor(input, res.bodyWriter, filter);

        res.contentType = "text/plain";
        res.headers["Content-Disposition"] = "attachment; filename=" ~ filePath.baseName;
        auto fileList = processor.list();

        foreach (file; fileList) {
                res.bodyWriter.write(file);
                res.bodyWriter.write("\0");
        }
}

void showArchive(string filePath, string urlPath,
                 HTTPServerRequest req, HTTPServerResponse res) {
        auto schemeAndAuthority = "http://127.0.0.1:8080"; //res.headers["SchemeAndAuthority"];
        auto internalPath = req.query.get("path");

        auto input = openFile(filePath);
        scope (exit) input.close();

        auto filter = new DirectoryFilter(internalPath);
        auto processor = new ArchiveProcessor(input, res.bodyWriter, filter);
        auto fileList = processor.list();

        Appender!(FileEntry[]) filesAppender;
        foreach (f; fileList) {
                auto name = f.baseName;
                if (f.endsWith('/')) name ~= '/';

                auto showUrl = urlPath ~ "?action=show&path=" ~ f.urlEncode;
                auto downloadUrl = urlPath ~ "?action=get&path=" ~ f.urlEncode;

                FileEntry fe = {name.endsWith('/'), name, 0, std.datetime.SysTime.init, showUrl, downloadUrl};
                filesAppender.put(fe);
        }

        auto internalParent = internalPath.pathSplitter.dropBack(1).buildPath;
        auto localParent = urlPath;
        if (internalPath.empty) {
                localParent = urlPath.pathSplitter.dropBack(1).buildPath.absolutePath("/");
        }

        auto currentPath = buildPath(urlPath, internalPath);
        auto parentUrl = schemeAndAuthority ~ localParent ~ "?action=show&path=" ~ internalParent.urlEncode;
        auto files = filesAppender.data;
        res.render!("template.dt", currentPath, parentUrl, files);
}

void showLocalDirectory(string filePath, string urlPath,
                        HTTPServerRequest req, HTTPServerResponse res) {
        string schemeAndAuthority = "http://127.0.0.1:8080"; //res.headers["SchemeAndAuthority"];

        Appender!(FileEntry[]) filesAppender;
        foreach (fi; iterateDirectory(filePath)) {
                auto name = fi.name;
                auto url = schemeAndAuthority ~ chainPath(urlPath, fi.name).to!string();
                auto downloadUrl = fi.name.endsWith(".zip") ? url : null;

                if (fi.isDirectory || fi.name.endsWith(".zip")) name ~= '/';
                if (fi.name.endsWith(".zip")) url ~= "?action=show";

                FileEntry fe = {fi.isDirectory, name, fi.size, fi.timeModified, url, downloadUrl};
                filesAppender.put(fe);
        }

        auto files = filesAppender.data;
        string currentPath = urlPath;
        string parentPath = urlPath == "/" ? null : Path(urlPath).parentPath.toString();
        string parentUrl = urlPath ? schemeAndAuthority ~ parentPath : null;
        res.render!("template.dt", currentPath, parentUrl, files);
}

void processRequest(HTTPServerRequest req, HTTPServerResponse res) {
        auto documentRoot = "/home/vt/".asNormalizedPath.to!string();
        auto urlPath = req.path.buildNormalizedPath.pathSplitter.stripLeft("..").buildPath.absolutePath("/");
        auto filePath = chainPath(documentRoot, urlPath.stripLeft('/')).to!string;

        if (!existsFile(filePath)) {
                return;
        }

        auto action = req.query.get("action");

        if (filePath.getFileInfo().isDirectory) {
                showLocalDirectory(filePath, urlPath, req, res);
        } else if (!action) {
                sendFile(req, res, Path(filePath));
        } else if (!filePath.endsWith(".zip")) {
                throw new HTTPStatusException(400, "Unsupported file type");
        }

        switch (action) {
        case "list": listArchive(filePath, req, res); break;
        case "get": getArchive(filePath, req, res); break;
        case "show": showArchive(filePath, urlPath, req, res); break;
        default: throw new HTTPStatusException(400, "Unkown action: " ~ action);
        }
}

shared static this() {
        try {
                ushort port = 8080;
                readOption!ushort("p", &port, "Port to listen on");

                auto settings = new HTTPServerSettings;
                settings.port = port;
                settings.bindAddresses = ["::1", "127.0.0.1"];
                listenHTTP(settings, &processRequest);
        } catch (Exception e) {
                stderr.writeln(e.msg ~ "\n");
        }
}
