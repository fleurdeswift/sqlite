//
//  SQLStatement.swift
//  SQL
//
//  Copyright © 2015 Fleur de Swift. All rights reserved.
//

import sqlite3
import ExtraDataStructures

internal typealias BindDeallocator = (@convention(c) (UnsafeMutablePointer<Void>) -> Void);

public final class SQLStatement {
    internal let database: SQLDatabase;
    internal var handle: COpaquePointer;
    
    internal init(database: SQLDatabase, handle: COpaquePointer) {
        self.database = database;
        self.handle   = handle;
    }
    
    deinit {
        if self.handle != COpaquePointer() {
            sqlite3_finalize(handle);
        }
    }
    
    // MARK: Bindings
    public func bind(value: Double, atIndex: Int) throws {
        let errorCode = sqlite3_bind_double(handle, Int32(atIndex), value);
        
        if errorCode != SQLITE_OK {
            throw SQLReportError(errorCode, handle: self.handle);
        }
    }
    
    public func bind(value: Double?, atIndex: Int) throws {
        var errorCode: Int32;
        
        if let value = value {
            errorCode = sqlite3_bind_double(handle, Int32(atIndex), value);
        }
        else {
            errorCode = sqlite3_bind_null(handle, Int32(atIndex));
        }
        
        if errorCode != SQLITE_OK {
            throw SQLReportError(errorCode, handle: self.handle);
        }
    }

    public func bind(value: Double, withName: String) throws {
        try bind(value, atIndex: Int(sqlite3_bind_parameter_index(handle, withName)));
    }

    public func bind(value: Double?, withName: String) throws {
        try bind(value, atIndex: Int(sqlite3_bind_parameter_index(handle, withName)));
    }

    public func bind(value: Int, atIndex: Int) throws {
        let errorCode = sqlite3_bind_int64(handle, Int32(atIndex), Int64(value));
        
        if errorCode != SQLITE_OK {
            throw SQLReportError(errorCode, handle: self.handle);
        }
    }

    public func bind(value: Int?, atIndex: Int) throws {
        var errorCode: Int32;

        if let value = value {
            errorCode = sqlite3_bind_int64(handle, Int32(atIndex), Int64(value));
        }
        else {
            errorCode = sqlite3_bind_null(handle, Int32(atIndex));
        }
        
        if errorCode != SQLITE_OK {
            throw SQLReportError(errorCode, handle: self.handle);
        }
    }
    
    public func bind(value: Int, withName: String) throws {
        try bind(value, atIndex: Int(sqlite3_bind_parameter_index(handle, withName)));
    }

    public func bind(value: Int?, withName: String) throws {
        try bind(value, atIndex: Int(sqlite3_bind_parameter_index(handle, withName)));
    }
    
    public func bind(value: String, atIndex: Int) throws {
        let errorCode = sqlite3_bind_text(handle, Int32(atIndex), value, -1, unsafeBitCast(-1, BindDeallocator.self));
        
        if errorCode != SQLITE_OK {
            throw SQLReportError(errorCode, handle: self.handle);
        }
    }

    public func bind(value: String?, atIndex: Int) throws {
        var errorCode: Int32;
    
        if let value = value {
            errorCode = sqlite3_bind_text(handle, Int32(atIndex), value, -1, unsafeBitCast(-1, BindDeallocator.self));
        }
        else {
            errorCode = sqlite3_bind_null(handle, Int32(atIndex));
        }
        
        if errorCode != SQLITE_OK {
            throw SQLReportError(errorCode, handle: self.handle);
        }
    }

    public func bind(value: NSData, atIndex: Int) throws {
        let errorCode = sqlite3_bind_blob(handle, Int32(atIndex), value.bytes, Int32(value.length), unsafeBitCast(-1, BindDeallocator.self));
        
        if errorCode != SQLITE_OK {
            throw SQLReportError(errorCode, handle: self.handle);
        }
    }

    public func bind(value: String, withName: String) throws {
        try bind(value, atIndex: Int(sqlite3_bind_parameter_index(handle, withName)));
    }

    public func bind(value: String?, withName: String) throws {
        try bind(value, atIndex: Int(sqlite3_bind_parameter_index(handle, withName)));
    }
    
    public func bind(value: NSData?, atIndex: Int) throws {
        var errorCode: Int32;
    
        if let value = value {
            errorCode = sqlite3_bind_blob(handle, Int32(atIndex), value.bytes, Int32(value.length), unsafeBitCast(-1, BindDeallocator.self));
        }
        else {
            errorCode = sqlite3_bind_null(handle, Int32(atIndex));
        }
        
        if errorCode != SQLITE_OK {
            throw SQLReportError(errorCode, handle: self.handle);
        }
    }
    
    public func bind(value: NSData, withName: String) throws {
        try bind(value, atIndex: Int(sqlite3_bind_parameter_index(handle, withName)));
    }

    public func bind(value: NSData?, withName: String) throws {
        try bind(value, atIndex: Int(sqlite3_bind_parameter_index(handle, withName)));
    }
    
    public func bindNull(atIndex: Int) throws {
        let errorCode = sqlite3_bind_null(handle, Int32(atIndex));
        
        if errorCode != SQLITE_OK {
            throw SQLReportError(errorCode, handle: self.handle);
        }
    }

    public func bindNull(withName: String) throws {
        let errorCode = sqlite3_bind_null(handle, sqlite3_bind_parameter_index(handle, withName));
        
        if errorCode != SQLITE_OK {
            throw SQLReportError(errorCode, handle: self.handle);
        }
    }
    
    public var bindCount: Int {
        get {
            return Int(sqlite3_bind_parameter_count(handle));
        }
    }
    
    public func bindIndex(name: String) -> Int {
        return Int(sqlite3_bind_parameter_index(handle, name));
    }

    public func bindName(index: Int) -> String {
        return String.fromCString(sqlite3_bind_parameter_name(handle, Int32(index)))!;
    }

    private var bindNamesCache: [String]?;
    public var bindNames: [String] {
        get {
            if let c = bindNamesCache {
                return c;
            }
            
            let bindCount = self.bindCount;
            var c         = [String]();
        
            c.reserveCapacity(bindCount);
        
            for (var index = 0; index < bindCount; ++index) {
                c.append(self.bindName(index));
            }
            
            bindNamesCache = c;
            return c;
        }
    }

    public func clearBindings() -> Void {
        sqlite3_clear_bindings(handle);
    }
    
    // MARK: Execution
    public func step() throws -> Bool {
        let errorCode = sqlite3_step(handle);
        
        if errorCode == SQLITE_ROW {
            return true;
        }
        else if (errorCode == SQLITE_OK) || (errorCode == SQLITE_DONE) {
            return false;
        }
        
        throw SQLReportError(errorCode, handle: self.handle);
    }
    
    public func reset() throws -> Void {
        let errorCode = sqlite3_reset(handle);
    
        if errorCode != SQLITE_OK {
            throw SQLReportError(errorCode, handle: self.handle);
        }
    }

    // MARK: Columns
    public var columnCount: Int {
        get {
            return Int(sqlite3_column_count(handle));
        }
    }
    
    public var dataCount: Int {
        get {
            return Int(sqlite3_data_count(handle));
        }
    }
    
    public func columnName(index: Int) -> String {
        return String.fromCString(sqlite3_column_name(handle, Int32(index)))!;
    }
    
    public func columnValue(index: Int) -> AnyObject? {
        let dataType = sqlite3_column_type(handle, Int32(index));
        
        if dataType == SQLITE_NULL {
            return nil;
        }
    
        switch (dataType) {
        case SQLITE_INTEGER:
            return Int(sqlite3_column_int64(handle, Int32(index)));
        case SQLITE_FLOAT:
            return Double(sqlite3_column_double(handle, Int32(index)));
        case SQLITE_BLOB:
            return NSData(bytes: sqlite3_column_blob(handle, Int32(index)), length: Int(sqlite3_column_bytes(handle, Int32(index))));
        default:
            return String.fromCString(UnsafePointer<CChar>(sqlite3_column_text(handle, Int32(index))));
        }
    }
    
    @warn_unused_result
    public func columnString(index: Int) -> String? {
        let dataType = sqlite3_column_type(handle, Int32(index));
        
        if dataType == SQLITE_NULL {
            return nil;
        }
        
        return String.fromCString(UnsafePointer<CChar>(sqlite3_column_text(handle, Int32(index))));
    }

    public func columnInt(index: Int) -> Int? {
        let dataType = sqlite3_column_type(handle, Int32(index));
        
        if dataType == SQLITE_NULL {
            return nil;
        }
        
        return Int(sqlite3_column_int64(handle, Int32(index)));
    }

    public func columnInt(index: Int) -> Int {
        return Int(sqlite3_column_int64(handle, Int32(index)));
    }

    public func columnDouble(index: Int) -> Double? {
        let dataType = sqlite3_column_type(handle, Int32(index));
        
        if dataType == SQLITE_NULL {
            return nil;
        }
        
        return Double(sqlite3_column_double(handle, Int32(index)));
    }

    public func columnDouble(index: Int) -> Double {
        return Double(sqlite3_column_double(handle, Int32(index)));
    }
    
    private var columnsNamesCache: [String]?;
    public var columnNames: [String] {
        get {
            if let c = columnsNamesCache {
                return c;
            }
            
            let columnCount = self.columnCount;
            var c = [String]();
        
            c.reserveCapacity(columnCount);
        
            for (var index = 0; index < columnCount; ++index) {
                c.append(self.columnName(index));
            }
            
            columnsNamesCache = c;
            return c;
        }
    }
}
