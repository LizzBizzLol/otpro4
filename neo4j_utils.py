from neo4j import GraphDatabase

class Neo4jHelper:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def add_user(self, user_id, screen_name, name, sex, home_town):
        with self.driver.session() as session:
            session.run(
                """
                MERGE (u:User {id: $id})
                SET u.screen_name = $screen_name, 
                    u.name = $name, 
                    u.sex = $sex, 
                    u.home_town = $home_town
                """,
                id=user_id, screen_name=screen_name, name=name, sex=sex, home_town=home_town
            )

    def add_relationship(self, user1_id, user2_id, relation_type):
        with self.driver.session() as session:
            session.run(
                """
                MATCH (u1:User {id: $user1_id}), (u2:User {id: $user2_id})
                MERGE (u1)-[:{relation_type}]->(u2)
                """.replace("{relation_type}", relation_type),
                user1_id=user1_id, user2_id=user2_id
            )

    def create_index(self, label, property_name):
        """
        Создаёт индекс для указанного свойства узлов с заданной меткой.
        Если индекс уже существует, он не будет дублироваться.
        """
        with self.driver.session() as session:
            session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.{property_name})")
