import std.algorithm.mutation: stripLeft;
import std.array: Appender, empty;
import std.conv: to;
import std.datetime: SysTime, UTC;
import std.file: getcwd;
import std.path: absolutePath, baseName, buildNormalizedPath, buildPath, chainPath, pathSplitter;
import std.range: dropBack;
import std.string: endsWith;

import vibe.core.args: readOption;
import vibe.core.file: FileStream, existsFile, getFileInfo, iterateDirectory, openFile;
import vibe.http.common: HTTPStatusException;
import vibe.http.fileserver: sendFile;
import vibe.http.server: HTTPServerRequest, HTTPServerResponse, HTTPServerSettings, listenHTTP, render;
import vibe.core.path: NativePath;
import vibe.textfilter.urlencode: urlEncode;

import archive: ArchiveFilter, AddDirectoryFilter, EglobFilter, PathFilter, sieveArchive;

import zip: LocalFile, UngetInputStream, parseAll;

struct FileEntry {
        bool isDirectory;
        string name;
        ulong size;
        SysTime timeModified;
	string url;
        string downloadUrl;
}

void getArchive(string filePath, HTTPServerRequest req, HTTPServerResponse res) {
        auto input = openFile(filePath);
        scope (exit) input.close();

        ArchiveFilter filter;
        if (auto eglob = req.query.get("eglob")) filter = new EglobFilter(eglob);
        else filter= new PathFilter(req.query.get("path"));

        res.contentType = "application/octet";
        res.headers["Content-Disposition"] = "attachment; filename=" ~ filePath.baseName;
        sieveArchive(input, res.bodyWriter, filter);
}

void showArchive(string filePath, string urlPath,
                 HTTPServerRequest req, HTTPServerResponse res) {
        auto urlPrefix = "http://" ~ req.host;

        auto internalPath = req.query.get("path");
        auto filter = new AddDirectoryFilter(internalPath);

        auto inputStream = openFile(filePath);
        scope (exit) inputStream.close();
        auto input = new UngetInputStream!FileStream(inputStream);

        Appender!(FileEntry[]) filesAppender;
        parseAll!LocalFile(input, delegate(LocalFile file) {
                        file.skipData(input);

                        auto fullPath = file.name;
                        if (!filter.match(fullPath)) return;

                        auto name = fullPath.baseName;
                        if (fullPath.endsWith('/')) name ~= '/';

                        auto showUrl = urlPath ~ "?action=show&path=" ~ fullPath.urlEncode;
                        auto downloadUrl = urlPath ~ "?action=get&path=" ~ fullPath.urlEncode;

                        FileEntry fe = {name.endsWith('/'), name, file.originalSize,
                                        file.modificationTime, showUrl, downloadUrl};
                        filesAppender.put(fe);
                });

        foreach (f; filter.additionalContent) {
                FileEntry fe = {f.endsWith('/'), f.baseName, 0, SysTime.fromUnixTime(0, UTC()),
                                urlPath ~ "?action=show&path=" ~ f.urlEncode};
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
        string parentPath = urlPath == "/" ? null : NativePath(urlPath).parentPath.toString();
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
                sendFile(req, res, NativePath(filePath));
        } else if (!filePath.endsWith(".zip")) {
                throw new HTTPStatusException(400, "Unsupported file type");
        }

        switch (action) {
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
                import std.stdio: stderr;
                stderr.writeln(e.msg ~ "\n");
        }
}
