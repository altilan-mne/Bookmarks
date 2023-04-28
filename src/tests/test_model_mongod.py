"""Test module for the Model module of Bookmark Manager, local standalone mongod implementation.

"""
import sys

from model_mongod import ModelMongod
from model_mongod import LOCAL_URI, COLLECTION_NAME
import exceptions


class TestModelMongod:
    """Testing class for standalone Mongo DB (mongod)."""

    md = ModelMongod()  # create an instance of the testing class

    def test_init_class(self):
        """Check if mongod connected to single and standalone Mongo server (mongod)."""
        addr_port = LOCAL_URI.split(":", 2)  # list [addr, port] og the connection
        address = (addr_port[0], int(addr_port[1]))  # tuple with address and port of connection
        assert self.md.client.address == address
        assert self.md.client.topology_description.topology_type_name == 'Single'
        # get the ServerDescription object
        serv_descr = self.md.client.topology_description.server_descriptions()[address]
        assert serv_descr.server_type_name == 'Standalone'

    def test_create_database(self):
        """Test of the database creation."""
        db_name = 'test_db'
        self.md.create_database(db_name)  # create test database and roots
        # fetch roots data
        result = self.md.bm.find_one()
        assert result['name'] == 'roots'
        assert len(str(result['_id'])) == 36
        assert result['parent_guid'] is None
        assert result['id_no'] == 0
        assert result['children'] == []
        assert result['date_added'] == result['date_modified']

        try:
            self.md.create_database(db_name)
        except FileExistsError as e:
            print('\nException FileExistsError raised successfully', e, file=sys.stderr)

    def test_delete_database(self):
        """Test of database deleting."""
        # drop existing test_db
        db_name = 'test_db'
        self.md.delete_database(db_name)
        assert db_name not in self.md.client.list_database_names()
        assert self.md.db is None
        assert self.md.bm is None

        # drop non-existing test_db
        try:
            self.md.delete_database(db_name)
        except FileNotFoundError as e:
            print('\nException FileNotFoundError raised successfully', e, file=sys.stderr)

    def test_open_database(self):
        """Test of database opening."""
        # try to open non-existing database
        try:
            self.md.open_database('not_existing')
        except FileNotFoundError as e:
            print('\nException FileNotFoundError raised successfully', e, file=sys.stderr)

        # create a new db
        db_name = 'test_db'
        self.md.create_database(db_name)
        # create another db
        self.md.create_database('another')

        # open existing database
        self.md.open_database(db_name)
        # fetch roots data
        result = self.md.bm.find_one()
        assert result['name'] == 'roots'
        assert len(str(result['_id'])) == 36
        assert result['parent_guid'] is None
        assert result['id_no'] == 0
        assert result['children'] == []
        assert result['date_added'] == result['date_modified']

        # drop unnecessary db
        self.md.client.drop_database('another')

    def test_add_node(self ):
        """Test of a node inserting.
        Database with 'roots' is opened."""
        # create attr_dict for the new folder
        new_folder = {'name': 'Folder1', 'parent_name': 'roots'}
        self.md.add_node(new_folder, node_type=True)  # create folder node

        # create attr_dict for the new url
        new_url = {'name': 'Url1', 'parent_name': 'Folder1', 'id_no': 2,
                   'url': 'URL', 'icon': 'ICON', 'keywords': 'KEYWORDS'}
        self.md.add_node(new_url, node_type=False)  # create url node

        # get guid of 'roots'
        roots_guid = self.md.bm.find_one({'name': 'roots'},
                                       {'_id': True},)  # return a dictionary
        # get guid of 'Folder1'
        folder_guid = self.md.bm.find_one({'name': 'Folder1'},
                                                {'_id': True}, )  # return a dictionary

        # get fields of the Url1
        res_url = self.md.bm.find_one({'name': 'Url1'})
        assert res_url['name'] == new_url['name']
        assert len(str(res_url['_id'])) == 36
        assert res_url['parent_guid'] == folder_guid['_id']
        assert res_url['id_no'] == new_url['id_no']
        assert res_url['url'] == new_url['url']
        assert res_url['icon'] == new_url['icon']
        assert res_url['keywords'] == new_url['keywords']

        # get fields of the Folder1
        res_folder = self.md.bm.find_one({'name': 'Folder1'})
        assert res_folder['name'] == new_folder['name']
        assert len(str(res_folder['_id'])) == 36
        assert res_folder['parent_guid'] == roots_guid['_id']
        assert res_folder['id_no'] == 0
        assert res_folder['children'] == [{res_url['name']: res_url['_id']}]
        assert res_folder['date_added'] == res_folder['date_modified']

    def test_get_node(self):
        """Test of node fields getting."""
        # try get non-existing node
        node_name = 'unknown'
        try:
            self.md.get_node(node_name)
        except exceptions.NodeNotExists as e:
            print('\nException NodeNotExists raised successfully', e, file=sys.stderr)

        # get guid of 'roots'
        roots_guid = self.md.bm.find_one({'name': 'roots'},
                                         {'_id': True}, )  # return a dictionary
        # get guid of 'Folder1'
        folder_guid = self.md.bm.find_one({'name': 'Folder1'},
                                          {'_id': True}, )  # return a dictionary

        # get Url1 node's fields
        node_name = 'Url1'
        node_url = self.md.get_node(node_name)
        assert node_url['name'] == 'Url1'
        assert len(node_url['guid']) == 36
        assert node_url['parent_guid'] == str(folder_guid['_id'])
        assert len(node_url['date_added']) == 19
        assert node_url['id_no'] == 2
        assert node_url['url'] == 'URL'
        assert node_url['icon'] == 'ICON'
        assert node_url['keywords'] == 'KEYWORDS'

        # get Url2 to the folder1, create attr_dict for the new url
        new_url = {'name': 'Url2', 'parent_name': 'Folder1', 'id_no': 3,
                   'url': 'URL', 'icon': 'ICON', 'keywords': 'KEYWORDS'}
        self.md.add_node(new_url, node_type=False)  # create url node

        # get Folder1 node's fields
        node_name = 'Folder1'
        node_folder = self.md.get_node(node_name)
        assert node_folder['name'] == 'Folder1'
        assert len(node_folder['guid']) == 36
        assert node_folder['parent_guid'] == str(roots_guid['_id'])
        assert len(node_folder['date_added']) == 19
        assert node_folder['date_modified'] == node_folder['date_added']
        assert node_folder['children'] == ['Url1', 'Url2']

    def test_get_children(self):
        """Test the method to get list of children names from the node."""
        # try to get from non-existing node
        node_name = 'does not exist'
        try:
            res = self.md.get_children(node_name)
        except exceptions.NodeNotExists as e:
            print('\nException NodeNotExists raised successfully', e, file=sys.stderr)

        # get children list from an url
        node_name = 'Url2'
        res = self.md.get_children(node_name)
        assert res == (False, ())

        # get children list from the Folder1
        node_name = 'Folder1'
        res = self.md.get_children(node_name)
        assert res == (True, ('Url1', 'Url2'))

    def test_update_node(self):
        """Test of the node updating."""
        # try update non-existing node
        try:
            self.md.update_node('unknown', {})
        except exceptions.NodeNotExists as e:
            print('\nException NodeNotExists raised successfully', e, file=sys.stderr)

        # update Folder1
        name = 'Folder1'
        attr_dict = {'name': 'FOLDER1', 'junk': 'JUNK'}
        self.md.update_node(name, attr_dict)
        result = self.md.bm.find_one({'name': 'FOLDER1'})
        assert 'junk' not in result  # if junk field was removed
        assert result['name'] == 'FOLDER1'
        # get date fields of the parent node
        parent_dates = self.md.bm.find_one ({'_id': result['parent_guid']},
                                            {'_id': False, 'date_added': True, 'date_modified': True}
        )
        assert len(str(parent_dates['date_modified'])) == 26  # milliseconds are not trimmed
        assert parent_dates['date_added'] < parent_dates['date_modified']  # parent is modified later then created

        # update Url1, keep node name, add the junk field 'wrong'
        name = 'Url1'
        attr_dict = {'url': 'new_url', 'icon': 'new_icon',
                     'keywords': 'k1, k2, k3', 'wrong': 'ERROR',
                     }
        self.md.update_node(name, attr_dict)
        result = self.md.bm.find_one({'name': name})
        assert 'wrong' not in result  # if junk field was removed
        assert result['name'] == 'Url1'
        assert result['url'] == attr_dict['url']
        assert result['icon'] == attr_dict['icon']
        assert result['keywords'] == attr_dict['keywords']
        # get date fields of the parent node
        parent_dates = self.md.bm.find_one ({'_id': result['parent_guid']},
                                            {'_id': False, 'date_added': True, 'date_modified': True}
        )
        assert len(str(parent_dates['date_modified'])) == 26  # milliseconds are not trimmed
        assert parent_dates['date_added'] < parent_dates['date_modified']  # parent is modified later then created

        # update name of Url1 with URL1
        attr_dict = {'name': 'URL1'}
        self.md.update_node(name, attr_dict)
        result = self.md.bm.find_one({'name': attr_dict['name']})
        assert result['name'] == attr_dict['name']

    def test_delete_node(self):
        """Test of node deleting."""
        # try delete non-existing node
        try:
            self.md.delete_node('unknown')
        except exceptions.NodeNotExists as e:
            print('\nException NodeNotExists raised successfully', e, file=sys.stderr)

        # try delete non-empty folder 'FOLDER1'
        try:
            self.md.delete_node('FOLDER1')
        except exceptions.FolderNotEmpty as e:
            print('\nException FolderNotEmpty raised successfully', e, file=sys.stderr)

        # delete 'URL1' node
        self.md.delete_node('URL1')
        # check if URL1 is removed from the collection
        res = self.md.bm.find_one({'name': 'URL1'})
        assert res is None
        # check if URL1 is deleted from children list of the FOLDER1
        res = self.md.bm.find_one({'name': 'FOLDER1'})
        assert len(res['children']) == 1  # only Url2 presents in the list

        # delete Url2 to clear FOLDER1
        self.md.delete_node('Url2')

        # now we can delete empty FOLDER1
        self.md.delete_node('FOLDER1')
        # check if FOLDER1 is removed from the collection
        res = self.md.bm.find_one({'name': 'FOLDER1'})
        assert res is None
        # check if FOLDER1 is deleted from children list of the 'roots'
        res = self.md.bm.find_one({'name': 'roots'})
        assert len(res['children']) == 0  # only Url2 presents in the list

        # delete database 'test_db' and close connection
        self.md.client.drop_database('test_db')
        self.md.client.close()