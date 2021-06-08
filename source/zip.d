import std.algorithm: min;
import std.array: empty;
import std.bitmanip: bitfields, littleEndianToNative, nativeToLittleEndian, peek;
import std.conv: to;
import std.datetime: DateTime, SysTime;
import std.system: Endian, endian;

import vibe.core.stream: InputStream, OutputStream, RandomAccessStream;

version (unittest) {
        import vibe.stream.memory: MemoryOutputStream, MemoryStream;

        import dunit.toolkit;
}

class UngetInputStream: InputStream {
private:
	InputStream mStream;
	ubyte[] mData;

public:
	this(InputStream stream) {
		mStream = stream;
	}

        void unget(ubyte[] data) {
		mData ~= data;
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

	}
}

bool parse(T)(UngetInputStream input, void delegate(T) process) {
        auto signature = input.get!uint();
        input.unget(signature.nativeToLittleEndian);
        if (signature != T.mHeader.MAGIC) return false;

        process(T(input));
        return true;
}

void parseAll(T)(UngetInputStream input, void delegate(T) process) {
        while (!input.empty) {
                auto r = parse!T(input, process);
                if (!r) return;
        }
}

struct LocalFile {
private:
        LocalFileHeader mHeader = void;
        ubyte[] mFileName = void;
        ExtraFields mExtraFields = void;

public:
        this(InputStream input) {
                input.read(mHeader.byteBuffer);
                assert (mHeader.signature.fromLittleEndian == LocalFileHeader.MAGIC);

                auto fileNameLength = mHeader.fileNameLength.fromLittleEndian;
                auto extraFieldLength = mHeader.extraFieldLength.fromLittleEndian;

                mFileName = new ubyte[fileNameLength];
                input.read(mFileName);

                mExtraFields = ExtraFields(input, extraFieldLength);
        }

        @property string name() const {
                return cast(string)mFileName;
        }

        @property ulong originalSize() const {
                auto size32 = mHeader.originalSize.fromLittleEndian;
                if (size32 != -1) return size32;

                auto field = mExtraFields.get!Zip64ExtraField();
                if (!field.exists) {
                        throw new Exception("Wrong extra field");
                }

                return field.originalSize;
        }

        @property ulong compressedSize() const {
                auto size32 = mHeader.compressedSize.fromLittleEndian;
                if (size32 != -1) return size32;

                auto field = mExtraFields.get!Zip64ExtraField();
                if (!field.exists) {
                        throw new Exception("Wrong extra field");
                }

                return field.compressedSize;
        }

        @property SysTime modificationTime() {
                return fromDosDateTime(mHeader.modificationDate, mHeader.modificationTime);
        }

        void skipData(InputStream input) {
                input.skip(this.compressedSize);
        }

        void write(OutputStream output) {
                output.write(mHeader.byteBuffer);
                output.write(mFileName);

                mExtraFields.write(output);
        }

        void writeData(InputStream input, OutputStream output) {
                auto s = this.compressedSize;
                if (s) output.write(input, s);
        }
}

// Test Zip64 local file header parsing
unittest {
        ubyte[] data = [
                // signature
                0x50, 0x4b, 0x03, 0x04,
                //version
                0x2d, 0x00,
                // flags
                0x00, 0x00,
                // compression
                0x08, 0x00,
                // modification time
                0x37, 0x6b,
                // modification date
                0x76, 0x47,
                // crc32
                0xc3, 0x38, 0x38, 0x19,
                // compressed size
                0xff, 0xff, 0xff, 0xff,
                // uncomressed size
                0xff, 0xff, 0xff, 0xff,
                // file name length
                0x04, 0x00,
                // extra field length
                0x30, 0x00,
                // file name
                0x7a, 0x65, 0x72, 0x6f,
                // extended timestamp
                0x55, 0x54, 0x09, 0x00, 0x03, 0x29, 0x98, 0x51, 0x56, 0x26, 0x98, 0x51, 0x56,
                // Info-ZIP UNIX (new)
                0x75, 0x78, 0x0b, 0x00, 0x01, 0x04, 0xe8, 0x03, 0x00, 0x00, 0x04, 0xe8, 0x03, 0x00,
                0x00,
                // Zip64 extended information extra field
                0x01, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x40, 0x01, 0x00, 0x00, 0x00, 0x50, 0x80,
                0x4f, 0x00, 0x00, 0x00, 0x00, 0x00];
        auto input = new UngetInputStream(new MemoryStream(data, false));

        auto timesCalled = 0;
        parse!LocalFile(input, delegate(LocalFile file) {
                        ++timesCalled;
                        file.name.assertEqual("zero");
                        file.originalSize.assertEqual(5368709120);
                        file.compressedSize.assertEqual(5210192);
                });
        timesCalled.assertEqual(1);
}

struct CentralDirectoryFile {
private:
        CentralDirectoryFileHeader mHeader = void;
        ubyte[] mFileName = void;
        ExtraFields mExtraFields = void;
        ubyte[] mFileComment = void;

public:
        this(InputStream input) {
                input.read(mHeader.byteBuffer);
                assert (mHeader.signature.fromLittleEndian == CentralDirectoryFileHeader.MAGIC);

                auto fileNameLength = mHeader.fileNameLength.fromLittleEndian;
                auto extraFieldLength = mHeader.extraFieldLength.fromLittleEndian;
                auto fileCommentLength = mHeader.fileCommentLength.fromLittleEndian;

                mFileName = new ubyte[fileNameLength];
                input.read(mFileName);

                mExtraFields = ExtraFields(input, extraFieldLength);

                mFileComment = new ubyte[fileCommentLength];
                input.read(mFileComment);
        }

        @property string name() const {
                return cast(string)mFileName;
        }

        @property ulong originalSize() const {
                auto size32 = mHeader.originalSize.fromLittleEndian;
                if (size32 != -1) return size32;

                auto field = mExtraFields.get!Zip64ExtraField();
                if (!field.exists) {
                        throw new Exception("Wrong extra field");
                }

                return field.originalSize;
        }

        @property ulong compressedSize() const {
                auto size32 = mHeader.compressedSize.fromLittleEndian;
                if (size32 != -1) return size32;

                auto field = mExtraFields.get!Zip64ExtraField();
                if (!field.exists) {
                        throw new Exception("Wrong extra field");
                }

                return field.compressedSize;
        }

        @property void localFileOffset(ulong offset) {
                if (offset < mHeader.localHeaderOffset.max) {
                        mHeader.localHeaderOffset = offset.to!uint.toLittleEndian();
                        return;
                }

                mHeader.localHeaderOffset = -1;
                auto field = mExtraFields.get!Zip64ExtraField();
                field.localHeaderOffset = offset;
        }

        void write(OutputStream output) {
                output.write(mHeader.byteBuffer);
                output.write(mFileName);
                mExtraFields.write(output);
                output.write(mFileComment);
        }
}

// Test Zip64 central directory file header parsing
unittest {
        ubyte[] data = [
                // signature
                0x50, 0x4b, 0x01, 0x02,
                // version made by
                0x1e, 0x03,
                // version needed to extract
                0x2d, 0x00,
                // flags
                0x00, 0x00,
                // compression method
                0x08, 0x00,
                // modification time
                0x37, 0x6b,
                // modification date
                0x76, 0x47,
                // crc32
                0xc3, 0x38, 0x38, 0x19,
                // compressed size
                0x50, 0x80, 0x4f, 0x00,
                // uncomressed size
                0xff, 0xff, 0xff, 0xff,
                // file name length
                0x04, 0x00,
                // extra field length
                0x24, 0x00,
                // file comment length
                0x00, 0x00,
                // disk number start
                0x00, 0x00,
                // internal file attributes
                0x00, 0x00,
                // external file attributes
                0x00, 0x00, 0xa4, 0x81,
                // relative offset of local header
                0x00, 0x00, 0x00, 0x00,
                // file name
                0x7a, 0x65, 0x72, 0x6f,
                // extended timestamp
                0x55, 0x54, 0x05, 0x00, 0x03, 0x29, 0x98, 0x51, 0x56,
                // Info-ZIP UNIX (new)
                0x75, 0x78, 0x0b, 0x00, 0x01, 0x04, 0xe8, 0x03, 0x00, 0x00, 0x04, 0xe8, 0x03, 0x00,
                0x00,
                // Zip64 extended information extra field
                0x01, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x40, 0x01, 0x00, 0x00, 0x00];
        auto input = new UngetInputStream(new MemoryStream(data, false));

        auto timesCalled = 0;
        parse!CentralDirectoryFile(input, delegate(CentralDirectoryFile file) {
                        ++timesCalled;
                        file.name.assertEqual("zero");
                        file.originalSize.assertEqual(5368709120);
                        file.compressedSize.assertEqual(5210192);
                });
        timesCalled.assertEqual(1);
}

struct Zip64EndOfCentralDirectoryRecord {
private:
        Zip64EndOfCentralDirectoryRecordHeader mHeader = void;
        ubyte[] mExtensibleData = void;

public:
        this(InputStream input) {
                input.read(mHeader.byteBuffer);
                assert (mHeader.signature.fromLittleEndian == Zip64EndOfCentralDirectoryRecordHeader.MAGIC);

                if (mHeader.sizeof - 12 < mHeader.size.fromLittleEndian) {
                        throw new Exception("Wrong size of Zip64EndOfCentralFirectoryRecord");
                }
                auto extensibleDataLength = mHeader.size.fromLittleEndian + 12 - mHeader.sizeof;

                mExtensibleData = new ubyte[extensibleDataLength];
                input.read(mExtensibleData);
        }

        @property void entriesCountOnThisDisk(ulong count) {
                mHeader.entriesCountOnThisDisk = count.toLittleEndian();
        }

        @property void entriesCount(ulong count) {
                mHeader.entriesCount = count.toLittleEndian();
        }

        @property void centralDirectorySize(ulong size) {
                mHeader.centralDirectorySize = size.toLittleEndian();
        }

        @property void centralDirectoryOffset(ulong offset) {
                mHeader.centralDirectoryOffset = offset.toLittleEndian();
        }

        void write(OutputStream output) {
                output.write(mHeader.byteBuffer);
                output.write(mExtensibleData);
        }
}

// Test Zip64 end of central directory record parsing
unittest {
        ubyte[] data = [
                // zip64 end of central dir signature
                0x50, 0x4b, 0x06, 0x06,
                // size of zip64 end of central directory record
                0x2c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // version made by
                0x1e, 0x03,
                // version needed to extract
                0x2d, 0x00,
                // number of this disk
                0x00, 0x00, 0x00, 0x00,
                // number of the disk with the start of the central directory
                0x00, 0x00, 0x00, 0x00,
                // total number of entries in the central directory on this disk
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // total number of entries in the central directory
                0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // size of the central directory
                0x56, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                // offset of start of central directory with respect to the starting disk number
                0xa2, 0x80, 0x4f, 0x00, 0x00, 0x00, 0x00, 0x00];
        auto input = new UngetInputStream(new MemoryStream(data, false));

        auto timesCalled = 0;
        parse!Zip64EndOfCentralDirectoryRecord(input, delegate(Zip64EndOfCentralDirectoryRecord record) {
                        ++timesCalled;

                        auto output = new MemoryOutputStream();
                        record.write(output);
                        output.data.assertEqual(data);
                });
        timesCalled.assertEqual(1);
}

struct Zip64EndOfCentralDirectoryLocator {
private:
        Zip64EndOfCentralDirectoryLocatorHeader mHeader = void;

public:
        this(InputStream input) {
                input.read(mHeader.byteBuffer);
                assert (mHeader.signature.fromLittleEndian == Zip64EndOfCentralDirectoryLocatorHeader.MAGIC);
        }

        @property void zip64EndOfCentralDirectoryRecordOffset(ulong offset) {
                mHeader.zip64EndOfCentralDirectoryRecordOffset = offset;
        }

        void write(OutputStream output) {
                output.write(mHeader.byteBuffer);
        }
}

// Test Zip64 end of central directory locator parsing
unittest {
        ubyte[] data = [
                // zip64 end of central dir locator signature
                0x50, 0x4b, 0x06, 0x07,
                // number of the disk with the start of the zip64 end of central directory
                0x00, 0x00, 0x00, 0x00,
                // relative offset of the zip64 end of central directory record
                0xf8, 0x80, 0x4f, 0x00, 0x00, 0x00, 0x00, 0x00,
                // total number of disks
                0x01, 0x00, 0x00, 0x00];
        auto input = new UngetInputStream(new MemoryStream(data, false));

        auto timesCalled = 0;
        parse!Zip64EndOfCentralDirectoryLocator(input, delegate(Zip64EndOfCentralDirectoryLocator record) {
                        ++timesCalled;

                        auto output = new MemoryOutputStream();
                        record.write(output);
                        output.data.assertEqual(data);
                });
        timesCalled.assertEqual(1);
}

struct EndOfCentralDirectoryRecord {
private:
        EndOfCentralDirectoryRecordHeader mHeader = void;
        ubyte[] mFileComment = void;

public:
        this(InputStream input) {
                input.read(mHeader.byteBuffer);
                assert (mHeader.signature.fromLittleEndian == EndOfCentralDirectoryRecordHeader.MAGIC);

                auto fileCommentLength = mHeader.fileCommentLength.fromLittleEndian;

                mFileComment = new ubyte[fileCommentLength];
                input.read(mFileComment);
        }

        @property void entriesCountOnThisDisk(ulong count) {
                if (count < mHeader.entriesCountOnThisDisk.max) {
                        mHeader.entriesCountOnThisDisk = count.to!ushort().toLittleEndian();
                } else {
                        mHeader.entriesCountOnThisDisk = -1.to!ushort;
                }
        }

        @property void entriesCount(ulong count) {
                if (count < mHeader.entriesCount.max) {
                        mHeader.entriesCount = count.to!ushort().toLittleEndian();
                } else {
                        mHeader.entriesCount = -1.to!ushort;
                }
        }

        @property void centralDirectorySize(ulong size) {
                if (size < mHeader.centralDirectorySize.max) {
                        mHeader.centralDirectorySize = size.to!int().toLittleEndian();
                } else {
                        mHeader.centralDirectorySize = -1;
                }
        }

        @property void centralDirectoryOffset(ulong offset) {
                if (offset < mHeader.centralDirectoryOffset.max) {
                        mHeader.centralDirectoryOffset = offset.to!int().toLittleEndian();
                } else {
                        mHeader.centralDirectoryOffset = -1;
                }
        }

        void write(OutputStream output) {
                output.write(mHeader.byteBuffer);
                output.write(mFileComment);
        }
}

// Test end of central directory record parsing
unittest {
        ubyte[] data = [
                // end of central dir signature
                0x50, 0x4b, 0x05, 0x06,
                // number of this disk
                0x00, 0x00,
                // number of the disk with the start of the central directory
                0x00, 0x00,
                // total number of entries in the central directory on this disk
                0x01, 0x00,
                // total number of entries in the central directory
                0x01, 0x00,
                // size of the central directory
                0x56, 0x00, 0x00, 0x00,
                // offset of start of central directory with respect to the starting disk number
                0xa2, 0x80, 0x4f, 0x00,
                // .ZIP file comment length
                0x00, 0x00];
        auto input = new UngetInputStream(new MemoryStream(data, false));

        auto timesCalled = 0;
        parse!EndOfCentralDirectoryRecord(input, delegate(EndOfCentralDirectoryRecord record) {
                        ++timesCalled;

                        auto output = new MemoryOutputStream();
                        record.write(output);
                        output.data.assertEqual(data);
                });
        timesCalled.assertEqual(1);
}

private struct ExtraFields {
private:
        ubyte[][ushort] mExtraFields = void;

public:
        this(InputStream input, size_t length) {
                while (length) {
                        auto headerId = input.get!ushort();
                        length -= headerId.sizeof;
                        assert (headerId !in mExtraFields);

                        auto size = input.get!ushort();
                        length -= size.sizeof;
                        assert (size <= length);

                        auto data = new ubyte[size];
                        input.read(data);

                        mExtraFields[headerId] = data;
                        length -= size;
                }
        }

        inout(T) get(T)() inout {
                auto p = T.MAGIC in mExtraFields;
                return inout(T)(p ? *p : null);
        }

        void write(OutputStream output) {
                foreach (headerId, field; mExtraFields) {
                        output.write(headerId.nativeToLittleEndian);
                        output.write(field.length.to!ushort.nativeToLittleEndian);
                        output.write(field);
                }
        }
}

private struct Zip64ExtraField {
        enum ushort MAGIC = 0x0001;

private:
        enum ORIGINAL_SIZE_OFFSET = 0x00;
        enum COMPRESSED_SIZE_OFFSET = 0x08;
        enum LOCAL_HEADER_OFFSET = 0x10;

        ubyte[] mData;

public:
        this(inout ubyte[] data) inout {
                mData = data;
        }

        @property bool exists() const {
                return mData != null;
        }

        @property ulong originalSize() const {
                if (mData.length < ORIGINAL_SIZE_OFFSET + ulong.sizeof) {
                        throw new Exception("Wrong zip64 extra field");
                }

                return mData[ORIGINAL_SIZE_OFFSET..$].peek!(ulong, Endian.littleEndian);
        }

        @property ulong compressedSize() const {
                if (mData.length < COMPRESSED_SIZE_OFFSET + ulong.sizeof) {
                        throw new Exception("Wrong zip64 extra field");
                }

                return mData[COMPRESSED_SIZE_OFFSET..$].peek!(ulong, Endian.littleEndian);
        }

        @property void localHeaderOffset(ulong offset) {
                if (mData.length < LOCAL_HEADER_OFFSET + ulong.sizeof) mData.length = ulong.sizeof * 3;
                mData[LOCAL_HEADER_OFFSET..LOCAL_HEADER_OFFSET+ulong.sizeof] = offset.nativeToLittleEndian;
        }
}

private align(1) struct LocalFileHeader {
        enum MAGIC = 0x04034B50;
        align(1):
        uint signature;
        ushort ver;
        ushort flags;
        ushort compression;
        DosTime modificationTime;
        DosDate modificationDate;
        uint crc32;
        uint compressedSize;
        uint originalSize;
        ushort fileNameLength;
        ushort extraFieldLength;
}

private align(1) struct CentralDirectoryFileHeader {
        enum MAGIC = 0x02014B50;
        align(1):
        uint signature;
        ushort ver;
        ushort versionNeeded;
        ushort flags;
        ushort compression;
        ushort modificationTime;
        ushort modificationDate;
        uint crc32;
        uint compressedSize;
        uint originalSize;
        ushort fileNameLength;
        ushort extraFieldLength;
        ushort fileCommentLength;
        ushort diskNumberStart;
        ushort internalAttributes;
        uint externalAttributes;
        uint localHeaderOffset;
}

private align(1) struct Zip64EndOfCentralDirectoryRecordHeader {
        enum MAGIC = 0x06064B50;
        align(1):
        uint signature;
        ulong size;
        ushort ver;
        ushort versionNeeded;
        uint diskNumber;
        uint diskNumberWithCentralDirectoryStart;
        ulong entriesCountOnThisDisk;
        ulong entriesCount;
        ulong centralDirectorySize;
        ulong centralDirectoryOffset;
}

private align(1) struct Zip64EndOfCentralDirectoryLocatorHeader {
        enum MAGIC = 0x07064B50;
        align(1):
        uint signature;
        uint diskNumberWithZip64EndOfCentralDirectoryRecord;
        ulong zip64EndOfCentralDirectoryRecordOffset;
        uint diskCount;
}

private align(1) struct EndOfCentralDirectoryRecordHeader {
        enum MAGIC = 0x06054B50;
        align(1):
        uint signature;
        ushort diskNumber;
        ushort diskNumberWithCentralDirectoryStart;
        ushort entriesCountOnThisDisk;
        ushort entriesCount;
        uint centralDirectorySize;
        uint centralDirectoryOffset;
        ushort fileCommentLength;
}

private align(1) struct DosDate {
        align(1):
        mixin(bitfields!(
                      uint, "day", 5,
                      uint, "month", 4,
                      uint, "years", 7));
}

private align(1) struct DosTime {
        align(1):
        mixin(bitfields!(
                      uint, "seconds", 5,
                      uint, "minute", 6,
                      uint, "hour", 5));
}

private T fromLittleEndian(T)(T val) {
        static if (endian == Endian.bigEndian) return swapEndian(val);
        else return val;
}

private T toLittleEndian(T)(T val) {
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

private SysTime fromDosDateTime(DosDate date, DosTime time) {
        auto dt = DateTime(1980 + date.years, date.month, date.day,
                           time.hour, time.minute, 2 * time.seconds);
        return SysTime(dt);
}
