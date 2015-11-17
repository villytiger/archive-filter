import std.algorithm.mutation: strip, stripLeft, stripRight;
import std.algorithm.sorting: multiSort;
import std.bitmanip: littleEndianToNative, nativeToLittleEndian;
import std.file: getcwd;
import std.functional: binaryReverseArgs;
import std.getopt;
import std.path: absolutePath, asNormalizedPath, baseName, buildNormalizedPath, chainPath,
        globMatch, pathSplitter;
import std.range: dropBack;
import std.range.primitives;
import std.stdio: stderr;

import vibe.d;
import vibe.stream.stdio: StderrStream, StdinStream, StdoutStream;

import archive: ArchiveFilter, ArchiveProcessor, DirectoryFilter, EglobFilter, PathFilter;

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
        auto input = openFile(filePath);
        scope (exit) input.close();

        ArchiveFilter filter;
        if (auto eglob = req.query.get("eglob")) filter = new EglobFilter(eglob);
        else filter= new PathFilter(req.query.get("path"));

        auto processor = new ArchiveProcessor(input, filter);

        res.contentType = "application/octet";
        res.headers["Content-Disposition"] = "attachment; filename=" ~ filePath.baseName;
        processor.sieve(res.bodyWriter);
}

void listArchive(string filePath, HTTPServerRequest req, HTTPServerResponse res) {
        string internalPath = req.query.get("path");

        auto input = openFile(filePath);
        scope (exit) input.close();

        auto filter = new DirectoryFilter(internalPath);
        auto processor = new ArchiveProcessor(input, filter);

        res.contentType = "text/plain";
        res.headers["Content-Disposition"] = "attachment; filename=" ~ filePath.baseName;
        auto fileList = processor.list();

        foreach (file; fileList) {
                res.bodyWriter.write(file.baseName);
                res.bodyWriter.write("\0");
        }
}

void showArchive(string filePath, string urlPath,
                 HTTPServerRequest req, HTTPServerResponse res) {
        auto urlPrefix = "http://" ~ req.host;
        auto internalPath = req.query.get("path");

        auto input = openFile(filePath);
        scope (exit) input.close();

        auto filter = new DirectoryFilter(internalPath);
        auto processor = new ArchiveProcessor(input, filter);
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
        auto parentUrl = urlPrefix ~ localParent ~ "?action=show&path=" ~ internalParent.urlEncode;
        auto files = filesAppender.data;
        res.render!("template.dt", currentPath, parentUrl, files);
}

void showLocalDirectory(string filePath, string urlPath,
                        HTTPServerRequest req, HTTPServerResponse res) {
        string urlPrefix = "http://" ~ req.host;

        Appender!(FileEntry[]) filesAppender;
        foreach (fi; iterateDirectory(filePath)) {
                auto name = fi.name;
                auto url = urlPrefix ~ chainPath(urlPath, fi.name).to!string();
                auto downloadUrl = fi.name.endsWith(".zip") ? url : null;

                if (fi.isDirectory || fi.name.endsWith(".zip")) {
                        name ~= '/';
                        url ~= "?action=show";
                }

                FileEntry fe = {fi.isDirectory, name, fi.size, fi.timeModified, url, downloadUrl};
                filesAppender.put(fe);
        }

        auto files = filesAppender.data;
        string currentPath = urlPath;
        string parentPath = urlPath == "/" ? null : Path(urlPath).parentPath.toString();
        string parentUrl = urlPath ? urlPrefix ~ parentPath ~ "?action=show" : null;
        res.render!("template.dt", currentPath, parentUrl, files);
}

void processRequest(HTTPServerRequest req, HTTPServerResponse res) {
        auto urlPath = req.path.buildNormalizedPath.pathSplitter.stripLeft("..").buildPath.absolutePath("/");
        auto filePath = chainPath(gDocumentRoot, urlPath.stripLeft('/')).to!string;

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

shared string gDocumentRoot;

shared static this() {
        try {
                ushort port = 8080;
                readOption!ushort("p", &port, "Port to listen on. Default is 8080.");

                string documentRoot = getcwd();
                readOption!string("r", &documentRoot,
                                  "Document root. Default is working directory.");
                gDocumentRoot = documentRoot.buildNormalizedPath;

                auto settings = new HTTPServerSettings;
                settings.port = port;
                settings.bindAddresses = ["::1", "127.0.0.1"];
                listenHTTP(settings, &processRequest);
        } catch (Exception e) {
                stderr.writeln(e.msg ~ "\n");
        }
}
