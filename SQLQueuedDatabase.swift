//
//  SQLQueuedDatabase.swift
//  SQL
//
//  Copyright © 2015 Fleur de Swift. All rights reserved.
//

import Foundation
import ExtraDataStructures

public class SQLRead {
    internal let database: SQLDatabase;

    internal init(_ database: SQLDatabase) {
        self.database = database;
    }

    public func prepare(sql: String) throws -> SQLStatement {
        return try database.prepare(sql);
    }

    public func exec(sql: String) throws -> Void {
        try database.exec(sql);
    }
}

public class SQLWrite : SQLRead {
    internal override init(_ database: SQLDatabase) {
        super.init(database);
    }
}

public class SQLQueuedDatabase {
    private let database: SQLDatabase;
    public  let queue = dispatch_queue_create("SQLQueuedDatabase", DISPATCH_QUEUE_CONCURRENT);

    public init(database: SQLDatabase) {
        self.database = database;
    }

    public convenience init(filename: String, flags: Int32, vfs: String?) throws {
        self.init(database: try SQLDatabase(filename: filename, flags: flags, vfs: vfs))
    }

    public func read(block: (SQLRead) throws -> Void) throws -> Void {
        try dispatch_sync(queue) {
            try block(SQLRead(self.database));
        }
    }

    public func read<T>(block: (SQLRead) throws -> T) throws -> T {
        return try dispatch_sync(queue) {
            return try block(SQLRead(self.database));
        }
    }

    public func readAsync(block: (SQLRead) -> Void) -> Void {
        dispatch_async(queue) {
            block(SQLRead(self.database));
        }
    }

    public func readAsync(block: (SQLRead) throws -> Void) -> Void {
        dispatch_async(queue) {
            do {
                try block(SQLRead(self.database));
            }
            catch {
                Swift.print("SQL Error: \(error)\n");
            }
        }
    }

    public func write(block: (SQLWrite) throws -> Void) throws -> Void {
        try dispatch_barrier_sync(queue) {
            do {
                try self.database.beginTransaction();
                try block(SQLWrite(self.database));
                try self.database.commit();
            }
            catch {
                do {
                    try self.database.rollback();
                }
                catch {
                }

                throw error;
            }
        }
    }

    public func write<T>(block: (SQLWrite) throws -> T) throws -> T {
        return try dispatch_barrier_sync(queue) {
            do {
                try self.database.beginTransaction();
                let result = try block(SQLWrite(self.database));
                try self.database.commit();
                return result;
            }
            catch {
                do {
                    try self.database.rollback();
                }
                catch {
                }

                throw error;
            }
        }
    }

    public func writeAsync(block: (SQLWrite) throws -> Void, errorBlock: (ErrorType) -> Void) -> Void {
        dispatch_barrier_async(queue) {
            do {
                try self.database.beginTransaction();
                try block(SQLWrite(self.database));
                try self.database.commit();
            }
            catch {
                do {
                    try self.database.rollback();
                }
                catch {
                }
                
                errorBlock(error);
            }
        }
    }
}
