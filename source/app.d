import std.algorithm.mutation: strip, stripLeft, stripRight;
import std.algorithm.sorting: multiSort;
import std.bitmanip: littleEndianToNative, nativeToLittleEndian;
import std.functional: binaryReverseArgs;
import std.getopt;
import std.path: baseName;
import std.range.primitives;

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
        this(Path path) {
                mPath = path.empty ? null : path.toString().strip('/');
        }

        bool match(string path) {
                if (mPath.empty) return true;
                else if (path.length < mPath.length) return false;
                else if (!path.startsWith(mPath)) return false;
                else if (mPath.length == path.length) return true;
                else if (path[mPath.length] == '/') return true;
                else return false;
        }
}

class DirectoryFilter: ArchiveFilter {
private:
        string mPath;

public:
        this(Path path) {
                mPath = path.empty ? null : path.toString().strip('/');
        }

        bool match(string path) {
                if (!path.startsWith(mPath)) {
                        return false;
                } else if (!mPath.empty) {
                        if (path.length < mPath.length + 2) return false;
                        else if (path[mPath.length] != '/') return false;
                        else path = path[mPath.length+1..$];
                }
                auto p = path.countUntil('/');
                return p == -1 || p + 1 == path.length;
        }
}

class ArchiveProcessor {
private:
	UngetInputStream mInput;
	OutputStream mOutput;
	ArchiveFilter mFilter;
	uint[string] mOffsets;

public:
	this(InputStream input, OutputStream output, ArchiveFilter filter) {
		mInput =  new UngetInputStream(input);
		mOutput = output;
		mFilter = filter;
	}

	void process() {
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

version (console) {
	void main(string[] args) {
		string inputFilePath, outputFilePath, filePath;
		auto helpInformation = getopt(args, "input|i", &inputFilePath, "output|o", &outputFilePath,
					      "file|f", &filePath);
		if (helpInformation.helpWanted || !filePath) {
			defaultGetoptPrinter("Archive filter. Allows to filter zip archive with path "
					     "to file or directory inside archive getting "
					     "smaller archive.",
					     helpInformation.options);
			return;
		}

		InputStream input;
		if (inputFilePath) input = openFile(inputFilePath);
		else input = new StdinStream;

		OutputStream output;
		if (outputFilePath) output = openFile(outputFilePath, FileMode.createTrunc);
		else output = new StdoutStream;

		auto filter = new DirectoryFilter(filePath);
		auto processor = new ArchiveProcessor(input, output, filter);

		processor.process();
		output.finalize();
	}
} else {
	shared static this() {
		auto settings = new HTTPServerSettings;
		settings.port = 8080;
		settings.bindAddresses = ["::1", "127.0.0.1"];
		listenHTTP(settings, &processRequest);

		logInfo("Please open http://127.0.0.1:8080/ in your browser.");
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

        void getArchive(Path filePath, Path internalPath,
                        HTTPServerRequest req, HTTPServerResponse res) {
                auto fileName = internalPath.empty ? filePath.head.toString() : internalPath.head.toString();
                res.contentType = "application/octet";
                res.headers["Content-Disposition"] = "attachment; filename=" ~ fileName ~ ".zip";

                auto input = openFile(filePath.toString().stripRight('/'));
                scope (exit) input.close();

                auto filter = new PathFilter(internalPath);
                auto processor = new ArchiveProcessor(input, res.bodyWriter, filter);
                processor.process();
        }

        void listArchive(Path filePath, Path internalPath,
                         HTTPServerRequest req, HTTPServerResponse res) {
                auto fileName = internalPath.empty ? filePath.head.toString() : internalPath.head.toString();
                res.contentType = "text/plain";
                res.headers["Content-Disposition"] = "attachment; filename=" ~ fileName ~ ".list";

                auto input = openFile(filePath.toString().stripRight('/'));
                scope (exit) input.close();

                auto filter = new DirectoryFilter(internalPath);
                auto processor = new ArchiveProcessor(input, res.bodyWriter, filter);
                auto fileList = processor.list();

                foreach (file; fileList) {
                        res.bodyWriter.write(file);
                        res.bodyWriter.write("\0");
                }
        }

        void processArchive(Path documentRoot, Path localPath, Path internalPath,
			    HTTPServerRequest req, HTTPServerResponse res) {
                string schemeAndAuthority = "http://127.0.0.1:8080"; //res.headers["SchemeAndAuthority"];
                auto absolutePath = documentRoot ~ localPath;
                absolutePath.endsWithSlash = false;

                if (internalPath.empty) internalPath.endsWithSlash = false;

                auto input = openFile(absolutePath);
                scope (exit) input.close();

                auto filter = new DirectoryFilter(internalPath);
                auto processor = new ArchiveProcessor(input, res.bodyWriter, filter);
                auto fileList = processor.list();

                Appender!(FileEntry[]) filesAppender;
                foreach (f; fileList) {
                        auto filePath = Path(f);
                        auto name = filePath.head.toString() ~ (filePath.endsWithSlash ? "/" : null);
                        auto url = schemeAndAuthority ~ '/' ~ (localPath ~ f).toString();
                        auto downloadUrl = url.stripRight('/') ~ "?q=get";

                        FileEntry fe = {filePath.endsWithSlash, name, 0, std.datetime.SysTime.init, url, downloadUrl};
                        filesAppender.put(fe);
                }

                Path currentPath = Path("/") ~ localPath ~ internalPath;
                string parentUrl = schemeAndAuthority ~ currentPath.parentPath.toString();
                auto files = sortFiles(filesAppender.data, req);
                res.render!("template.dt", currentPath, parentUrl, files, humanReadableInteger);
        }

	void processLocalPath(Path documentRoot, Path path,
			      HTTPServerRequest req, HTTPServerResponse res) {
		Path absolutePath = documentRoot ~ path;
		string schemeAndAuthority = "http://127.0.0.1:8080"; //res.headers["SchemeAndAuthority"];

		Appender!(FileEntry[]) filesAppender;
		foreach (fi; iterateDirectory(absolutePath)) {
                        auto url = schemeAndAuthority ~ '/' ~ (path ~ fi.name).toString();
                        auto downloadUrl = fi.name.endsWith(".zip") ? url : null;

			if (fi.isDirectory || fi.name.endsWith(".zip")) {
                                fi.name ~= '/';
                                url ~= "/";
                        }

			FileEntry fe = {fi.isDirectory, fi.name, fi.size, fi.timeModified, url, downloadUrl};
			filesAppender.put(fe);
		}

		Path currentPath = Path("/") ~ path;
		string parentUrl = path.empty ? null : schemeAndAuthority ~ currentPath.parentPath.toString();
                auto files = sortFiles(filesAppender.data, req);
		res.render!("template.dt", currentPath, parentUrl, files, humanReadableInteger);
	}

	void processRequest(HTTPServerRequest req, HTTPServerResponse res) {
		auto documentRoot = Path("/home/vt/");
		auto path = splitPath(documentRoot, Path(req.path.stripLeft('/')));
                auto q = req.query.get("q");

                if (q) {
                        if (q == "list") {
                                listArchive(documentRoot ~ path.local, path.internal, req, res);
                        } else if (q == "get") {
                                getArchive(documentRoot ~ path.local, path.internal, req, res);
                        }
		} else if (path.local.endsWithSlash && !existsFile(documentRoot ~ path.local)
                           && path.local.head.toString().endsWith(".zip")) {
			processArchive(documentRoot, path.local, path.internal, req, res);
                } else if (path.internal.empty) {
			auto absolutePath = documentRoot ~ path.local;
			if (absolutePath.getFileInfo().isDirectory) {
				processLocalPath(documentRoot, path.local, req, res);
			} else {
				sendFile(req, res, absolutePath);
			}

                }
        }
}
