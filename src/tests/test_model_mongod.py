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

        # drop test_db
        self.md.client.drop_database(db_name)