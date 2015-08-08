//
//  SQL.swift
//  SQL.h
//
//  Copyright © 2015 Fleur de Swift. All rights reserved.
//

import sqlite3
import ExtraDataStructures

public enum SQLError : ErrorType {
    case Constraint(description: String)
    case Generic(code: Int32, description: String)
}

internal func SQLReportError(errorCode: Int32) -> SQLError {
    switch (errorCode) {
    case SQLITE_CONSTRAINT:
        return SQLError.Constraint(description: String.fromCString(sqlite3_errstr(errorCode))!);
    default:
        return SQLError.Generic(code: errorCode, description: String.fromCString(sqlite3_errstr(errorCode))!)
    }
}

internal func SQLReportError(errorCode: Int32, handle: COpaquePointer) -> SQLError {
    switch (errorCode) {
    case SQLITE_CONSTRAINT:
        return SQLError.Constraint(description: String.fromCString(sqlite3_errstr(errorCode))!);
    default:
        return SQLError.Generic(code: errorCode, description: String.fromCString(sqlite3_errmsg(handle))!)
    }
}

public let SQL_OPEN_READONLY = SQLITE_OPEN_READONLY;
public let SQL_OPEN_READWRITE = SQLITE_OPEN_READWRITE;
public let SQL_OPEN_CREATE = SQLITE_OPEN_CREATE;
public let SQL_OPEN_NOMUTEX = SQLITE_OPEN_NOMUTEX;
public let SQL_OPEN_FULLMUTEX = SQLITE_OPEN_FULLMUTEX;
public let SQL_OPEN_PRIVATECACHE = SQLITE_OPEN_PRIVATECACHE;
public let SQL_OPEN_URI = SQLITE_OPEN_URI;
