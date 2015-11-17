import std.algorithm: canFind, min, stripRight;
import std.array: Appender, empty;
import std.bitmanip: littleEndianToNative, nativeToLittleEndian, peek;
import std.conv: to;
import std.path: globMatch;
import std.range.primitives: popFront, popFrontN;
import std.string: endsWith, indexOf, startsWith;
import std.system: Endian, endian;
import std.typecons: scoped;

import vibe.core.stream: InputStream, OutputStream, RandomAccessStream;
import vibe.stream.counting: CountingOutputStream;

static immutable PKZIP_LOCAL_FILE_HEADER_MAGIC = [0x50, 0x4b ,0x03, 0x04];
static immutable PKZIP_CENTRAL_DIRECTORY_FILE_HEADER_MAGIC = [0x50, 0x4b ,0x01, 0x02];
static immutable PKZIP_END_OF_CENTRAL_DIRECTORY_RECORD_MAGIC = [0x50, 0x4b ,0x05, 0x06];
static immutable  PKZIP_ZIP64_END_OF_CENTRAL_DIRECTORY_RECORD_MAGIC = [0x50, 0x4b ,0x06, 0x06];
static immutable  PKZIP_ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_MAGIC = [0x50, 0x4b ,0x06, 0x07];

class ArchiveProcessor {
private:
	UngetInputStream mInput;
	ArchiveFilter mFilter;
        uint[string] mOffsets;

        void processLocalFiles(void delegate(scope LocalFile) process) {
                while (!mInput.empty()) {
                        auto signature = mInput.get!uint();
                        mInput.unget(signature.nativeToLittleEndian);
                        if (signature != LocalFileHeader.MAGIC) return;

                        auto localFile = scoped!LocalFile(mInput);

                        process(localFile);
                }
        }

        void processCentralDirectory(ulong[string] offsets, OutputStream output) {
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

			header[42..46] = nativeToLittleEndian(offsets[fileName].to!uint);

			output.write(header);
			output.write(fileName);
			if (extraFieldLength + fileCommentLength) output.write(mInput, extraFieldLength + fileCommentLength);
		}
        }

        void processZip64EndOfCentralDirectoryRecord(ulong totalEntries,
                                                     ulong centralDirectoryOffset,
                                                     ulong centralDirectorySize,
                                                     OutputStream output) {
                ubyte[56] header;
                mInput.read(header[0..4]);
                if (header[0..4] != PKZIP_ZIP64_END_OF_CENTRAL_DIRECTORY_RECORD_MAGIC) {
                        mInput.unget(header[0..4]);
                        return;
                } else {
                        mInput.read(header[4..$]);
                }

                header[24..32] = nativeToLittleEndian(totalEntries);
                header[32..40] = nativeToLittleEndian(totalEntries);
                header[40..48] = nativeToLittleEndian(centralDirectorySize);
                header[48..56] = nativeToLittleEndian(centralDirectoryOffset);

                output.write(header);
        }

        void processZip64EndOfCentralDirectoryLocator(ulong endOfCentralDirectory,
                                                      OutputStream output) {
                ubyte[20] header;
                mInput.read(header[0..4]);
                if (header[0..4] != PKZIP_ZIP64_END_OF_CENTRAL_DIRECTORY_LOCATOR_MAGIC) {
                        mInput.unget(header[0..4]);
                        return;
                } else {
                        mInput.read(header[4..$]);
                }

                header[8..16] = nativeToLittleEndian(endOfCentralDirectory);

                output.write(header);
        }

public:
	this(InputStream input, ArchiveFilter filter) {
		mInput =  new UngetInputStream(input);
		mFilter = filter;
	}

        void sieve(OutputStream outputStream) {
                auto output = new CountingOutputStream(outputStream);

                ulong[string] offsets;
                processLocalFiles(delegate(scope LocalFile file) {
                                auto name = file.name;
                                if (mFilter.match(name)) {
                                        offsets[name] = output.bytesWritten;
                                        file.write(output);
                                } else {
                                        file.skip();
                                }
                        });

                ulong centralDirectoryOffset = output.bytesWritten;
                processCentralDirectory(offsets, output);
		ulong centralDirectorySize = output.bytesWritten - centralDirectoryOffset;

                processZip64EndOfCentralDirectoryRecord(offsets.length, centralDirectoryOffset,
                                                        centralDirectorySize, output);
                processZip64EndOfCentralDirectoryLocator(centralDirectoryOffset + centralDirectorySize,
                                                         output);

		ubyte[22] header;
                mInput.read(header);
		if (header[0..PKZIP_END_OF_CENTRAL_DIRECTORY_RECORD_MAGIC.length] != PKZIP_END_OF_CENTRAL_DIRECTORY_RECORD_MAGIC) {
			return;
			//TODO throw
		}

		header[8..10] = nativeToLittleEndian(offsets.length.to!ushort);
		header[10..12] = nativeToLittleEndian(offsets.length.to!ushort);
		header[12..16] = nativeToLittleEndian(centralDirectorySize.to!int);
		header[16..20] = nativeToLittleEndian(centralDirectoryOffset.to!int);

		output.write(header);
		output.write(mInput);
	}

	string[] list() {
		Appender!(string[]) result;

                processLocalFiles(delegate(scope LocalFile file) {
                                auto name = file.name;
                                if (mFilter.match(name)) result.put(name);

                                file.skip();
                        });

		return result.data;
        }
}

// Test Zip64 local file header parsing
private unittest {
        import vibe.stream.memory: MemoryStream;

        ubyte[] data = [
                0x50, 0x4b, 0x03, 0x04, 0x2d, 0x00, 0x00, 0x00, 0x08, 0x00, 0xf3, 0xb2,
                0x70, 0x47, 0xc3, 0x38, 0x38, 0x19, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
                0xff, 0xff, 0x08, 0x00, 0x30, 0x00, 0x7a, 0x65, 0x72, 0x6f, 0x2e, 0x74,
                0x78, 0x74, 0x55, 0x54, 0x09, 0x00, 0x03, 0x39, 0x2d, 0x4a, 0x56, 0x35,
                0x2d, 0x4a, 0x56, 0x75, 0x78, 0x0b, 0x00, 0x01, 0x04, 0xe8, 0x03, 0x00,
                0x00, 0x04, 0xe8, 0x03, 0x00, 0x00, 0x01, 0x00, 0x10, 0x00, 0x00, 0x00,
                0x00, 0x40, 0x01, 0x00, 0x00, 0x00, 0x50, 0x80, 0x4f, 0x00, 0x00, 0x00,
                0x00, 0x00];
        auto input = new MemoryStream(data, false);

        auto filter = new PathFilter("zero.txt");
        auto archive = new ArchiveProcessor(input, filter);

        auto timesCalled = 0;
        archive.processLocalFiles(delegate(scope LocalFile localFile) {
                        ++timesCalled;
                        assert (localFile.originalSize == 5368709120);
                        assert (localFile.compressedSize == 5210192);
                });
        assert(timesCalled == 1);
}

class LocalFile {
private:
        UngetInputStream mInput;
        LocalFileHeader mHeader = void;
        ubyte[] mFileName;
        ubyte[][ushort] mExtraFields;

public:
        this(UngetInputStream input) {
                mInput = input;

                mInput.read(mHeader.byteBuffer);
                assert (mHeader.signature.fromLittleEndian == LocalFileHeader.MAGIC);

                auto fileNameLength = mHeader.fileNameLength.fromLittleEndian;
                auto extraFieldLength = mHeader.extraFieldLength.fromLittleEndian;

                mFileName = new ubyte[fileNameLength];
                mInput.read(mFileName);

                while (extraFieldLength) {
                        auto headerId = mInput.get!ushort();
                        extraFieldLength -= headerId.sizeof;
                        assert (headerId !in mExtraFields);

                        auto size = mInput.get!ushort();
                        extraFieldLength -= size.sizeof;
                        assert (size <= extraFieldLength);

                        auto data = new ubyte[size];
                        mInput.read(data);

                        mExtraFields[headerId] = data;
                        extraFieldLength -= size;
                }
        }

        @property string name() const {
                return cast(string)mFileName;
        }

        @property ulong originalSize() const {
                auto size32 = mHeader.uncompressedSize.fromLittleEndian;
                if (size32 != -1) return size32;

                auto magic = Zip64ExtraField.MAGIC;
                if (magic !in mExtraFields) {
                        throw new Exception("Wrong extra field");
                }

                auto field = scoped!Zip64ExtraField(mExtraFields[magic]);
                return field.originalSize;
        }

        @property ulong compressedSize() const {
                auto size32 = mHeader.compressedSize.fromLittleEndian;
                if (size32 != -1) return size32;

                auto magic = Zip64ExtraField.MAGIC;
                if (magic !in mExtraFields) {
                        throw new Exception("Wrong extra field");
                }

                auto field = scoped!Zip64ExtraField(mExtraFields[magic]);
                return field.compressedSize;
        }

        void skip() {
                mInput.skip(this.compressedSize);
        }

        void write(OutputStream output) {
                output.write(mHeader.byteBuffer);
                output.write(mFileName);

                foreach (headerId, field; mExtraFields) {
                        output.write(headerId.nativeToLittleEndian);
                        output.write(field.length.to!ushort.nativeToLittleEndian);
                        output.write(field);
                }

                auto s = this.compressedSize;
                if (s) output.write(mInput, s);
        }
}

private align(1) struct LocalFileHeader {
        enum MAGIC = 0x04034B50;
        align(1):
        uint signature;
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

class Zip64ExtraField {
        public enum ushort MAGIC = 0x0001;
        enum ORIGINAL_SIZE_OFFSET = 0x00;
        enum COMPRESSED_SIZE_OFFSET = 0x08;

        const ubyte[] mData;

public:
        this(const ubyte[] data) {
                mData = data;
        }

        @property ulong originalSize() {
                return mData[ORIGINAL_SIZE_OFFSET..$].peek!(ulong, Endian.littleEndian);
        }

        @property ulong compressedSize() {
                return mData[COMPRESSED_SIZE_OFFSET..$].peek!(ulong, Endian.littleEndian);
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
                mPath = path.stripRight('/');
        }

        bool match(string path) {
                if (!path.startsWith(mPath)) return false;
                else if (path.length == mPath.length) return true;
                else return path[mPath.length] == '/';
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

class EglobFilter: ArchiveFilter {
private:
        string mPattern;

public:
        this(string pattern) {
                mPattern = pattern;
        }

        bool match(string path) {
                string s = path;
                for (string p = mPattern; !p.empty;) {
                        if (p.startsWith("**")) {
                                p.popFrontN(2);
                                if (p.empty) return true;

                                auto i = p.indexOf('*');
                                if (i == -1) return s.endsWith(p);
                                else if (i == 0) return false;

                                auto j = s.indexOf(p[0..i]);
                                if (j == -1) return false;

                                p.popFrontN(i);
                                s.popFrontN(j + i);
                        } else if (p.startsWith("*")) {
                                p.popFront();
                                if (p.empty) return !s.canFind('/');

                                auto i = p.indexOf('*');
                                if (i == -1) {
                                        if (!s.endsWith(p)) return false;
                                        else if (s.length < p.length) return false;
                                        else return !s[0..$-p.length].canFind('/');
                                }

                                auto j = s.indexOf(p[0..i]);
                                if (j == -1) return false;
                                else if (s[0..j].canFind('/')) return false;

                                p.popFrontN(i);
                                s.popFrontN(j + i);
                        } else {
                                auto i = p.indexOf('*');
                                if (i == -1) return p == s;
                                else if (s.length < i) return false;
                                else if (s[0..i] != p[0..i]) return false;

                                p.popFrontN(i);
                                s.popFrontN(i);
                        }
                }

                return s.empty;
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

private T fromLittleEndian(T)(T val) {
        static if (endian == Endian.bigEndian) return swapEndian(val);
        else return val;
}

private type get(type)(InputStream stream) {
	ubyte[type.sizeof] result;
	stream.read(result);
	return littleEndianToNative!type(result);
}

private void skip(InputStream stream, size_t length) {
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

private ubyte[] byteBuffer(Type)(ref Type v) {
        return (cast(ubyte*)&v)[0..v.sizeof];
}

private class UngetInputStream: InputStream {
private:
	InputStream mStream;
	ubyte[] mData;
        ulong mCount = 0;

public:
	this(InputStream stream) {
		mStream = stream;
	}

        @property inout(InputStream) sourceStream() inout { return mStream; }

        @property ulong count() const { return mCount; }

        void unget(ubyte[] data) {
		mData ~= data;
                mCount -= data.length;
	}

	override @property bool empty() { return mData.empty() && mStream.empty(); }

	override @property ulong leastSize() { return mData.length + mStream.leastSize(); }

	override @property bool dataAvailableForRead() { return !mData.empty || mStream.dataAvailableForRead(); }

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

                mCount += dst.length;
	}
}
