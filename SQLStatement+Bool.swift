//
//  SQLStatement+Bool.swift
//  SQL
//
//  Copyright © 2015 Fleur de Swift. All rights reserved.
//

import sqlite3

public extension SQLStatement {
    public func bind(value: Bool, atIndex: Int) throws {
        let errorCode = sqlite3_bind_int(handle, Int32(atIndex), value ? Int32(1): Int32(0));
        
        if errorCode != SQLITE_OK {
            throw SQLReportError(errorCode, handle: self.handle);
        }
    }

    public func bind(value: Bool?, atIndex: Int) throws {
        var errorCode: Int32;

        if let value = value {
            errorCode = sqlite3_bind_int(handle, Int32(atIndex), value ? Int32(1): Int32(0));
        }
        else {
            errorCode = sqlite3_bind_null(handle, Int32(atIndex));
        }
        
        if errorCode != SQLITE_OK {
            throw SQLReportError(errorCode, handle: self.handle);
        }
    }
    
    public func bind(value: Bool, withName: String) throws {
        try bind(value, atIndex: Int(sqlite3_bind_parameter_index(handle, withName)));
    }

    public func bind(value: Bool?, withName: String) throws {
        try bind(value, atIndex: Int(sqlite3_bind_parameter_index(handle, withName)));
    }

    public func columnBool(index: Int) -> Bool? {
        let dataType = sqlite3_column_type(handle, Int32(index));
        
        if dataType == SQLITE_NULL {
            return nil;
        }
        
        return sqlite3_column_int(handle, Int32(index)) != 0 ? true: false;
    }

    public func columnBool(index: Int) -> Bool {
        return sqlite3_column_int(handle, Int32(index)) != 0 ? true: false;
    }
}
