"""A Model part of Bookmark Manager implemented with Mongo DB (document type)


"""

from pymongo import MongoClient

local_URI = 'mongodb://localhost:27017'

def main():
    """Test Mongo DB interface.

    """
    client = MongoClient(local_URI)  # create a client and connect to the running MongoDB server (mongod)
    db = client.testing  # connect to the existing database
    db1 = client.newtest  # new database 'newtest' will be created when the first document be added
    coll = db.my_tree  # connect to the existing collection  (aka table for SQL)
    coll1 = db1.test  # new collection 'test' will be created when the first document be added
    names = db.list_collection_names()
    print(names)

    # document presents in Python as a dictionary: by key, value pairs
    # document is an item aka row in SLQ
    doc1 = {'name': 'folder', 'type_id': True, 'children': []}  # a document

    # insert a document into collection
    # into existing collection
    # insert_one() returns an instance of InsertOneResult
    # InsertOneResult().inserted_id returns an id of the inserted doc
    # InsertOneResult().acknowledged returns if writing was acknowledged (when concerning about writing result)
    doc1_id = coll.insert_one(doc1) # return an instance
    print(doc1_id, doc1_id.inserted_id, doc1_id.acknowledged)

    # into non-existing collection
    doc1_id_1 = coll1.insert_one(doc1)  # newtest database, coll1 collection and doc1 created at the first insertion to db1
    doc1_id = db.test.insert_one(doc1)  # insert doc1 into the new collection 'test'

    names = db.list_collection_names()
    print(names)

    print(coll.find_one())  # fetch one result directly

    for d in coll.find({}):  # fetch all documents by cursor, not documents
        print(d)

    # delete collection 'test' in database 'newtest'
    # drop_collection() returns a dict, pair {'ok': 1.0} - success, pair {'ok': 0.0} - error
    # there are another pairs with the information of dropping
    drop_coll1 = db1.drop_collection("test")
    print(drop_coll1)  # {'nIndexesWas': 1, 'ns': 'newtest.test', 'ok': 1.0} returned

    drop_coll1 = db1.drop_collection("test")
    print(drop_coll1)  # {'ok': 0.0, 'errmsg': 'ns not found', 'code': 26, 'codeName': 'NamespaceNotFound'} returned

    # delete 'doc1' from 'my_test' collection
    # collection.delete_one() returns an instance of results.DeleteResult
    # this instance has 3 properties: deleted_count, acknowledged, raw_result == {'n': n_deleted, 'ok': 1.0}
    res = coll.delete_one(doc1)
    print(res, res.deleted_count, res.raw_result, res.acknowledged)  # 1 {'n': 1, 'ok': 1.0} True returned

    res = coll.delete_one(doc1)
    print(res, res.deleted_count, res.raw_result, res.acknowledged)  # 0 {'n': 0, 'ok': 1.0} True returned, 0 means not found

    res = db.test.delete_one(doc1)
    print(res, res.deleted_count, res.raw_result, res.acknowledged)  # 1 {'n': 1, 'ok': 1.0} True returned

    # insert several documents in one command to server
    url1 = {'name': 'URL1', 'url': 'http://google.com'}
    url2 = {'name': 'URL2', 'url': 'http://microsoft.com'}
    folder = {'name': 'folder1', 'children': [url1, url2]}
    doc_list = [folder, url1, url2]
    result = coll.insert_many(doc_list)
    print(result, result.inserted_ids)

    # querying more the one object
    # collection.find() returns the Cursor for interation over all matching documents
    for doc in coll.find({"url": {"$exists": True}}, {"url": True, '_id': 0}):
        print(doc)

if __name__ == '__main__':
    main()