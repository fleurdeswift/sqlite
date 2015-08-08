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
        let statement   = try prepare(sql);
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

    public func exec(sql: String, @noescape callback: (data: [String: AnyObject]) -> Bool) throws -> Void {
        let statement   = try prepare(sql);
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
        let statement = try prepare(sql);
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
    
    public func exec(queue: dispatch_queue_t, sql: String, block: (statement: SQLStatement) throws -> Void) throws -> Void {
        try dispatch_sync(queue) {
            try block(statement: try self.prepare(sql));
        }
    }
    
    public func exec<T>(queue: dispatch_queue_t, sql: String, block: (statement: SQLStatement) throws -> T) throws -> T {
        return try dispatch_sync(queue) {
            return try block(statement: try self.prepare(sql));
        }
    }

    public func exec<T>(queue: dispatch_queue_t, sql: String, block: (statement: SQLStatement) throws -> T?) throws -> T? {
        return try dispatch_sync(queue) {
            return try block(statement: try self.prepare(sql));
        }
    }
    
    public func transactionAsync(queue: dispatch_queue_t, block: () throws -> Void, errorBlock: (error: ErrorType) -> Void) -> Void {
        dispatch_barrier_async(queue) {
            do {
                try self.beginTransaction();
                try block();
                try self.commit();
            }
            catch {
                do {
                    try self.rollback();
                }
                catch {
                }
                
                errorBlock(error: error);
            }
        }
    }
    
    public func transaction(queue: dispatch_queue_t, block: () throws -> Void) throws -> Void {
        try dispatch_barrier_sync(queue) {
            do {
                try self.beginTransaction();
                try block();
                try self.commit();
            }
            catch {
                do {
                    try self.rollback();
                }
                catch {
                }
                
                throw error;
            }
        }
    }

    public func transaction<T>(queue: dispatch_queue_t, block: () throws -> T) throws -> T {
        return try dispatch_barrier_sync(queue) {
            do {
                try self.beginTransaction();
                let result = try block();
                try self.commit();
                return result;
            }
            catch {
                do {
                    try self.rollback();
                }
                catch {
                }
                
                throw error;
            }
        }
    }

    public func transaction<T>(queue: dispatch_queue_t, block: () throws -> T?) throws -> T? {
        return try dispatch_barrier_sync(queue) {
            do {
                try self.beginTransaction();
                let result = try block();
                try self.commit();
                return result;
            }
            catch {
                do {
                    try self.rollback();
                }
                catch {
                }
                
                throw error;
            }
        }
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
