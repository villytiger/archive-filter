import std.algorithm: canFind, setDifference, stripRight;
import std.array: Appender, array, empty;
import std.container.rbtree: RedBlackTree, redBlackTree;
import std.path: globMatch;
import std.range.primitives: popFront, popFrontN;
import std.string: endsWith, indexOf, startsWith;

import vibe.core.stream: isInputStream, isOutputStream;
import vibe.stream.counting: createCountingOutputStream;

import zip: CentralDirectoryFile, EndOfCentralDirectoryRecord, LocalFile, createUngetInputStream,
        Zip64EndOfCentralDirectoryLocator, Zip64EndOfCentralDirectoryRecord, parse, parseAll;

void sieveArchive(InputStream, OutputStream)(InputStream inputStream, OutputStream outputStream, ArchiveFilter filter)
		if (isInputStream!InputStream && isOutputStream!OutputStream)
{
        auto input = createUngetInputStream(inputStream);
        auto output = createCountingOutputStream(outputStream);

        ulong[string] offsets;
        parseAll!LocalFile(input, delegate(LocalFile file) {
                        auto name = file.name;
                        if (!filter.match(name)) {
                                file.skipData(input);
                                return;
                        }

                        offsets[name] = output.bytesWritten;

                        file.write(output);
                        file.writeData(input, output);
                });

        ulong centralDirectoryOffset = output.bytesWritten;

        parseAll!CentralDirectoryFile(input, delegate(CentralDirectoryFile file) {
                        auto name = file.name;
                        if (!filter.match(name)) return;

                        file.localFileOffset = offsets[name];
                        file.write(output);
                });

        ulong centralDirectorySize = output.bytesWritten - centralDirectoryOffset;
        ulong zip64EndOfCentralDirectoryRecordOffset = centralDirectoryOffset + centralDirectorySize;

        parse!Zip64EndOfCentralDirectoryRecord(input, delegate(Zip64EndOfCentralDirectoryRecord record) {
                        record.entriesCountOnThisDisk = offsets.length;
                        record.entriesCount = offsets.length;
                        record.centralDirectorySize = centralDirectorySize;
                        record.centralDirectoryOffset = centralDirectoryOffset;
                        record.write(output);
                });

        parse!Zip64EndOfCentralDirectoryLocator(input, delegate(Zip64EndOfCentralDirectoryLocator locator) {
                        locator.zip64EndOfCentralDirectoryRecordOffset = zip64EndOfCentralDirectoryRecordOffset;
                        locator.write(output);
                });

        parse!EndOfCentralDirectoryRecord(input, delegate(EndOfCentralDirectoryRecord record) {
                        record.entriesCountOnThisDisk = offsets.length;
                        record.entriesCount = offsets.length;
                        record.centralDirectorySize = centralDirectorySize;
                        record.centralDirectoryOffset = centralDirectoryOffset;
                        record.write(output);
                });
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

class BaseDirectoryFilter: ArchiveFilter {
protected:
        string mPath;

public:
        this(string path) {
                mPath = path;
                if (!path.empty && !path.endsWith('/')) mPath ~= '/';
        }

        bool match(string path) {
                if (path[mPath.length..$].stripRight('/').canFind('/')) return false;
                else return true;
        }
}

class DirectoryFilter: BaseDirectoryFilter {
public:
        this(string path) {
                super(path);
        }

        override bool match(string path) {
                if (!path.startsWith(mPath) || path.length == mPath.length) return false;
                else return super.match(path);
        }
}

class AddDirectoryFilter: BaseDirectoryFilter {
private:
        RedBlackTree!string mActualContent;
        RedBlackTree!string mAdditionalContent;

public:
        this(string path) {
                super(path);
                mActualContent = new RedBlackTree!string();
                mAdditionalContent = new RedBlackTree!string();
        }

        override bool match(string path) {
                if (!path.startsWith(mPath) || path.length == mPath.length) {
                        return false;
                } else if (super.match(path)) {
                        mActualContent.insert!string(path);
                        return true;
                } else {
                        auto i = path.indexOf('/', mPath.length);
                        mAdditionalContent.insert!string(path[0..i+1]);
                        return false;
                }
        }

        @property string[] additionalContent() const {
                return cast(string[])array(setDifference(mAdditionalContent[], mActualContent[]));
        }
}
