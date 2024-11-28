from neo4j import GraphDatabase

class Neo4jHelper:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def add_user(self, user_id, screen_name, name, sex, home_town):
        with self.driver.session() as session:
            session.run(
                "MERGE (u:User {id: $id}) "
                "SET u.screen_name = $screen_name, u.name = $name, u.sex = $sex, u.home_town = $home_town",
                id=user_id, screen_name=screen_name, name=name, sex=sex, home_town=home_town
            )

    def add_relationship(self, user1_id, user2_id, relation_type):
        with self.driver.session() as session:
            session.run(
                "MATCH (u1:User {id: $user1_id}), (u2:User {id: $user2_id}) "
                f"MERGE (u1)-[:{relation_type}]->(u2)",
                user1_id=user1_id, user2_id=user2_id
            )
