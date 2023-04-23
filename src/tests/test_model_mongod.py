"""Test module for the Model module of Bookmark Manager, local standalone mongod implementation.

"""
import sys
from model_mongod import ModelMongod
from model_mongod import LOCAL_URI, COLLECTION_NAME

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
