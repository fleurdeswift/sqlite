//
//  String+SQL.swift
//  SQL
//
//  Copyright © 2015 Fleur de Swift. All rights reserved.
//

public extension String {
    public var asLikeClause: String {
        get {
            let ws = NSCharacterSet.whitespaceAndNewlineCharacterSet();
            var s  = self;
        
            while let range = s.rangeOfCharacterFromSet(ws) {
                s.replaceRange(range, with: "%");
            }
            
            return "%" + s + "%";
        }
    }
}

