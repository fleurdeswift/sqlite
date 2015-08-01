//
//  SQL.swift
//  SQL.h
//
//  Copyright © 2015 Fleur de Swift. All rights reserved.
//

import sqlite3
import ExtraDataStructures

public enum SQLError : ErrorType {
    case GenericError(errorCode: Int32, errorDescription: String?)
}

internal func SQLReportError(errorCode: Int32) -> SQLError {
    switch (errorCode) {
    default:
        return SQLError.GenericError(errorCode: errorCode, errorDescription: String.fromCString(sqlite3_errstr(errorCode)))
    }
}

internal func SQLReportError(errorCode: Int32, handle: COpaquePointer) -> SQLError {
    switch (errorCode) {
    default:
        return SQLError.GenericError(errorCode: errorCode, errorDescription: String.fromCString(sqlite3_errmsg(handle)))
    }
}

public let SQL_OPEN_READONLY = SQLITE_OPEN_READONLY;
public let SQL_OPEN_READWRITE = SQLITE_OPEN_READWRITE;
public let SQL_OPEN_CREATE = SQLITE_OPEN_CREATE;
public let SQL_OPEN_NOMUTEX = SQLITE_OPEN_NOMUTEX;
public let SQL_OPEN_FULLMUTEX = SQLITE_OPEN_FULLMUTEX;
public let SQL_OPEN_PRIVATECACHE = SQLITE_OPEN_PRIVATECACHE;
public let SQL_OPEN_URI = SQLITE_OPEN_URI;

public func SQLOpen(filename: String, flags: Int32, vfs: String?) throws -> SQLDatabase {
    var opaque = COpaquePointer();
    var errorCode: Int32;

    if let vfs = vfs {
        errorCode = sqlite3_open_v2(filename, &opaque, flags, vfs);
    }
    else {
        errorCode = sqlite3_open_v2(filename, &opaque, flags, nil);
    }
    
    if errorCode != SQLITE_OK {
        throw SQLReportError(errorCode);
    }
    
    return SQLDatabase(handle: opaque);
}
