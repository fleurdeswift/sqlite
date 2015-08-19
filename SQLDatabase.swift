//
//  SQLDatabase.swift
//  SQL
//
//  Copyright © 2015 Fleur de Swift. All rights reserved.
//

import sqlite3
import ExtraDataStructures

public final class SQLDatabase {
    internal var handle: COpaquePointer;

    internal init(handle: COpaquePointer) {
        self.handle = handle;
    }
    
    public convenience init(filename: String, flags: Int32, vfs: String?) throws {
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
        
        self.init(handle: opaque);
    }
    
    deinit {
        if self.handle != COpaquePointer() {
            sqlite3_close_v2(self.handle);
        }
    }
    
    public func prepare(sql: String) throws -> SQLStatement {
        var statement = COpaquePointer();
        let errorCode = sqlite3_prepare_v2(handle, sql, -1, &statement, nil);
        
        if errorCode != SQLITE_OK {
            throw SQLReportError(errorCode, handle: self.handle);
        }
        
        return SQLStatement(database: self, handle: statement);
    }

    public func exec(sql: String, @noescape callback: (data: [String: String]) -> Bool) throws -> Void {
        let statement   = try self.prepare(sql);
        var first       = true;
        var columns     = [String]();
        var columnCount = 0;
        var values      = [String: String]();
        
        while try statement.step() {
            if (first) {
                first       = false;
                columnCount = statement.columnCount;
                columns     = statement.columnNames;
            }
            
            for (var index = 0; index < columnCount; ++index) {
                let value: String? = statement.columnString(index);
            
                if let value = value {
                    values[columns[index]] = value;
                }
                else {
                    values.removeValueForKey(columns[index]);
                }
            }
            
            callback(data: values);
        }
    }

    public func exec(sql: String, callback: (data: [String: AnyObject]) -> Bool) throws -> Void {
        let statement   = try self.prepare(sql);
        var first       = true;
        var columns     = [String]();
        var columnCount = 0;
        var values      = [String: AnyObject]();
        
        while try statement.step() {
            if (first) {
                first       = false;
                columnCount = statement.columnCount;
                columns     = statement.columnNames;
            }
            
            for (var index = 0; index < columnCount; ++index) {
                let value = statement.columnValue(index);
                
                if let value = value {
                    values[columns[index]] = value;
                }
                else {
                    values.removeValueForKey(columns[index]);
                }
            }
            
            callback(data: values);
        }
    }

    public func exec(sql: String) throws -> Void {
        let statement = try self.prepare(sql);
        try statement.step();
    }

    public func beginTransaction() throws -> Void {
        try exec("BEGIN TRANSACTION;");
    }
    
    public func commit() throws -> Void {
        try exec("COMMIT TRANSACTION;");
    }

    public func rollback() throws -> Void {
        try exec("ROLLBACK TRANSACTION;");
    }

    public var changes: Int {
        get {
            return Int(sqlite3_changes(handle));
        }
    }

    public var totalChanges: Int {
        get {
            return Int(sqlite3_total_changes(handle));
        }
    }
}
