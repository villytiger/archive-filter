import std.bitmanip: littleEndianToNative, nativeToLittleEndian;
import std.getopt;
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

class PathFilter {
private:
        string mPath;

public:
        this(string path) {
                mPath = path.back == '/' ? path[0..$-1] : path;
        }

        bool match(string path) {
                if (mPath.length > path.length) return false;
                return path.startsWith(mPath)
                        && (path.length == mPath.length || path[mPath.length] == '/');
        }
}

class ArchiveProcessor {
private:
	UngetInputStream mInput;
	OutputStream mOutput;
	PathFilter mFilter;
	uint[string] mOffsets;

public:
	this(InputStream input, OutputStream output, PathFilter filter) {
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
				return null;
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
			mOutput.write(fileName ~ "\n");
		}

		mOutput.write(result.data.to!string ~ "\n");

		return result.data;
	}
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

		auto filter = new PathFilter(filePath);
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

	auto splitPath(string path) {
		auto pathArray = path.split('/');
		for (auto i = 0; i != pathArray.length; ++i) {
			auto s = pathArray[0..$-i].join('/');
			if (existsFile(s)) {
				auto internal = pathArray[$-i..$].join('/');
				return tuple!("file", "internal")(s, internal);
			}
		}

		return tuple!("file", "internal")("", "");
	}

	void processRequest(HTTPServerRequest req, HTTPServerResponse res) {
		auto q = req.query.get("q");

		auto path = splitPath(req.path);
		if (!path.file) return;

		if (q && q == "read") {
			res.contentType = "application/octet";

			auto input = openFile(path.file);
			scope (exit) input.close();

			auto filter = new PathFilter(path.internal);
			auto processor = new ArchiveProcessor(input, res.bodyWriter, filter);
			processor.process();
		} else if (path.internal && path.internal.back == '/') {
			res.contentType = "text/plain";

			auto input = openFile(path.file);
			scope (exit) input.close();

			auto filter = new PathFilter(path.internal);
			auto processor = new ArchiveProcessor(input, res.bodyWriter, filter);
			auto fileList = processor.list();

			res.bodyWriter.write("sdfdfg");
			foreach (fileName; fileList) {
				res.bodyWriter.write("asd\n");
				res.bodyWriter.write(fileName);
				//res.bodyWriter.write("\0");
			}
		} else {
			res.bodyWriter.write(path.file ~ '\n');
			res.bodyWriter.write(path.internal ~ '\n');
		}
        }
}
