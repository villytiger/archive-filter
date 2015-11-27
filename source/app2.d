import std.algorithm: canFind, stripRight;
import std.array: Appender, empty;
import std.path: dirName, globMatch;
import std.range.primitives: popFront, popFrontN;
import std.string: endsWith, indexOf, startsWith;

import vibe.core.file: FileMode, createDirectory, existsFile, openFile;
import vibe.core.stream: InputStream, OutputStream;
import vibe.stream.counting: CountingOutputStream;
import vibe.stream.zlib: DeflateInputStream;

import zip: CentralDirectoryFile, CompressionMethod, EndOfCentralDirectoryRecord, LocalFile,
        UngetInputStream, Zip64EndOfCentralDirectoryLocator, Zip64EndOfCentralDirectoryRecord,
        parse, parseAll;

import core.atomic;
import etc.c.zlib;
import std.algorithm;
import std.exception;
import std.functional;
import vibe.utils.array;
import vibe.core.core;

class ZipInputStream : InputStream {
	import std.zlib;
	private {
		InputStream m_in;
		z_stream m_zstream;
		FixedRingBuffer!(ubyte, 4096) m_outbuffer;
		ubyte[1024] m_inbuffer;
		bool m_finished = false;
		ulong m_ninflated, n_read;
                ulong m_bytesLeft;
	}

	enum HeaderFormat {
		gzip,
		deflate,
		automatic
	}

	this(InputStream src, ulong compressedSize)
	{
		m_in = src;
                m_bytesLeft = compressedSize;
		if (m_in.empty || m_bytesLeft == 0) {
			m_finished = true;
		} else {
			zlibEnforce(inflateInit2(&m_zstream, -15));
			readChunk();
		}
	}

	@property bool empty() { return this.leastSize == 0; }

	@property ulong leastSize()
	{
		assert(!m_finished || m_bytesLeft == 0, "Input contains more data than expected.");
		if (m_outbuffer.length > 0) return m_outbuffer.length;
		if (m_finished) return 0;
		readChunk();
		assert(m_outbuffer.length || m_finished);
		return m_outbuffer.length;
	}

	@property bool dataAvailableForRead()
	{
		return m_outbuffer.length > 0;
	}

	const(ubyte)[] peek() { return m_outbuffer.peek(); }

	void read(ubyte[] dst)
	{
		enforce(dst.length == 0 || !empty, "Reading empty stream");

		while (dst.length > 0) {
			auto len = min(m_outbuffer.length, dst.length);
			m_outbuffer.read(dst[0 .. len]);
			dst = dst[len .. $];

			if (!m_outbuffer.length && !m_finished) readChunk();
			enforce(dst.length == 0 || m_outbuffer.length || !m_finished, "Reading past end of zlib stream.");
		}
	}

	void readChunk()
	{
		assert(m_outbuffer.length == 0, "Buffer must be empty to read the next chunk.");
		assert(m_outbuffer.peekDst().length > 0);
		enforce (!m_finished, "Reading past end of zlib stream.");

		m_zstream.next_out = m_outbuffer.peekDst().ptr;
		m_zstream.avail_out = cast(uint)m_outbuffer.peekDst().length;

		while (!m_outbuffer.length) {
			if (m_zstream.avail_in == 0) {
				auto clen = min(m_inbuffer.length, m_bytesLeft);
				m_in.read(m_inbuffer[0 .. clen]);
                                m_bytesLeft -= clen;
				m_zstream.next_in = m_inbuffer.ptr;
				m_zstream.avail_in = cast(uint)clen;
			}
			auto avins = m_zstream.avail_in;
			//logInfo("inflate %s -> %s (@%s in @%s)", m_zstream.avail_in, m_zstream.avail_out, m_ninflated, n_read);
			auto ret = zlibEnforce(inflate(&m_zstream, Z_SYNC_FLUSH));
			//logInfo("    ... %s -> %s", m_zstream.avail_in, m_zstream.avail_out);
			assert(m_zstream.avail_out != m_outbuffer.peekDst.length || m_zstream.avail_in != avins);
			m_ninflated += m_outbuffer.peekDst().length - m_zstream.avail_out;
			n_read += avins - m_zstream.avail_in;
			m_outbuffer.putN(m_outbuffer.peekDst().length - m_zstream.avail_out);
			assert(m_zstream.avail_out == 0 || m_zstream.avail_out == m_outbuffer.peekDst().length);

			if (ret == Z_STREAM_END) {
				m_finished = true;
				assert(m_bytesLeft == 0, "Input expected to be empty at this point.");
				return;
			}
		}
	}
}

private int zlibEnforce(int result)
{
	switch (result) {
		default:
			if (result < 0) throw new Exception("unknown zlib error");
			else return result;
		case Z_ERRNO: throw new Exception("zlib errno error");
		case Z_STREAM_ERROR: throw new Exception("zlib stream error");
		case Z_DATA_ERROR: throw new Exception("zlib data error");
		case Z_MEM_ERROR: throw new Exception("zlib memory error");
		case Z_BUF_ERROR: throw new Exception("zlib buffer error");
		case Z_VERSION_ERROR: throw new Exception("zlib version error");
	}
}

shared int taskCount = 0;

void decompress(string archivePath, ulong position, ulong compressedSize,
                CompressionMethod compressionMethod, string outputPath, ulong originalSize) {
        import std.stdio;
        auto input = openFile(archivePath);
        scope (exit) input.close();
        input.seek(position);

        InputStream wrappedInput;
        final switch (compressionMethod) {
        case CompressionMethod.none: wrappedInput = input; break;
        case CompressionMethod.deflate: wrappedInput = new ZipInputStream(input, compressedSize); break;
        }

        auto output = openFile(outputPath, FileMode.createTrunc);
        scope (exit) output.close();

        output.write(wrappedInput, originalSize);

        atomicOp!("-=")(taskCount, 1);
}

void unpackArchive(string archivePath) {
        auto inputStream = openFile(archivePath);
        scope (exit) inputStream.close();

        auto input = new UngetInputStream(inputStream);

        parseAll!LocalFile(input, delegate(LocalFile file) {
                        auto dir = file.name.dirName;
                        if (!existsFile(dir)) createDirectory(dir);

                        if (file.name.endsWith('/')) {
                                if (!existsFile(file.name)) createDirectory(file.name);
                                return;
                        }

                        while (atomicLoad(taskCount) > 12) {
                        }

                        runWorkerTaskH(&decompress, archivePath, input.count(), file.compressedSize,
                                      file.compressionMethod, file.name, file.originalSize);
                        input.skip(file.compressedSize);

                        atomicOp!("+=")(taskCount, 1);

                        /*InputStream wrappedInput;
                        final switch (file.compressionMethod) {
                        case CompressionMethod.none: wrappedInput = input; break;
                        case CompressionMethod.deflate: wrappedInput = new ZipInputStream(input, file.compressedSize); break;
                        }

                        auto output = openFile(file.name, FileMode.createTrunc);
                        scope (exit) output.close();*/

                        //output.write(wrappedInput);
                        //file.writeOriginalData(wrappedInput, output);

                        /*if (file.compressionMethod == CompressionMethod.none) {
                                auto output = openFile(file.name, FileMode.createTrunc);
                                scope (exit) output.close();
                                file.writeData(input, output);
                        } else {
                                import vibe.core.core;
                                runWorkerTask(&decompress, input, file.name, file.compressedSize, file.originalSize);
                                }*/
                });

        processEvents();
        exitEventLoop();
}

void main(string[] args) {
        runTask(toDelegate(&unpackArchive), args[1]);
        runEventLoop();
}
