from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from bdi_api.settings import Settings
from neo4j import GraphDatabase

settings = Settings()

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)


class PersonCreate(BaseModel):
    name: str
    city: str
    age: int


class RelationshipCreate(BaseModel):
    from_person: str
    to_person: str
    relationship_type: str = "FRIENDS_WITH"

@s7.post("/graph/person")
def create_person(person: PersonCreate) -> dict:
    """Create a person node in Neo4J.

    Use the BDI_NEO4J_URL environment variable to configure the connection.
    Start Neo4J with: make neo4j
    """
    # TODO: Connect to Neo4J using neo4j.GraphDatabase.driver(settings.neo4j_url, auth=(settings.neo4j_user, settings.neo4j_password))
    driver=GraphDatabase.driver(settings.neo4j_url, auth=(settings.neo4j_user, settings.neo4j_password))

    # TODO: Create a Person node with the given properties
    with driver.session() as session:
        session.run("""CREATE (p:Person {name: $name, city: $city, age: $age})""",
                    name=person.name, city=person.city, age=person.age) 
    # TODO: Return {"status": "ok", "name": person.name}
    return {"status": "ok", "name": person.name}


@s7.get("/graph/persons")
def list_persons() -> list[dict]:
    """List all person nodes.

    Each result should include: name, city, age.
    """
    # TODO: Connect to Neo4J
    driver=GraphDatabase.driver(
        settings.neo4j_url, 
        auth=(settings.neo4j_user, settings.neo4j_password))

    # TODO: MATCH (p:Person) RETURN p
    with driver.session() as session: 
        result=session.run("MATCH (p:Person) RETURN p")
    # TODO: Return list of dicts with name, city, age
        persons=[{"name":record['p']['name'],
             'city':record['p']['city'],
             'age':record['p']['age'],}
             for record in result]
    return persons


@s7.get("/graph/person/{name}/friends")
def get_friends(name: str) -> list[dict]:
    """Get friends of a person.

    Returns all persons connected by a FRIENDS_WITH relationship (any direction).
    If person not found, return 404.
    """
    # TODO: Connect to Neo4J
    driver=GraphDatabase.driver(settings.neo4j_url, auth=(settings.neo4j_user, settings.neo4j_password))

    # TODO: First check if person exists, return 404 if not
    with driver.session() as session:
        exists=session.run("MATCH (p:Person {name: $name}) RETURN p",
                           name=name).single()
        if not exists:
            raise HTTPException(status_code=404, detail=f"Person '{name}' not found")
        
    # TODO: MATCH (p:Person {name: name})-[:FRIENDS_WITH]-(friend:Person)
        result =session.run(
            """MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend:Person) 
            RETURN friend""",
            name=name
    )
    # TODO: Return list of friend dicts with name, city, age
        return [{'name':record['friend']['name'],
             'city':record['friend']['city'],
             'age':record['friend']['age'],
             }
             for record in result]


@s7.post("/graph/relationship")
def create_relationship(rel: RelationshipCreate) -> dict:
    """Create a relationship between two persons.

    Both persons must exist. Returns 404 if either is not found.
    """
    # TODO: Connect to Neo4J
    driver=GraphDatabase.driver(settings.neo4j_url, auth=(settings.neo4j_user, settings.neo4j_password))

    # TODO: Verify both persons exist
    with driver.session() as session:
        result=session.run(
            """MATCH (a:Person {name: $from}), (b:Person {name: $to}) RETURN a, b""",
            **{"from":rel.from_person, #the **{} help to pass dictionary keys as parameters
            "to":rel.to_person}
        ).single()

        if not result:
            raise HTTPException(status_code=404, detail='One or both persons not found')
    # TODO: CREATE (a)-[:FRIENDS_WITH]->(b)
        session.run(
            """MATCH (a:Person {name: $from}), (b:Person {name: $to})
            CREATE (a)-[:FRIENDS_WITH]->(b)""",
            **{"from":rel.from_person,
            "to":rel.to_person}
    )
    # TODO: Return {"status": "ok", "from": rel.from_person, "to": rel.to_person}
        return {"status": "ok", "from": rel.from_person, "to": rel.to_person}


@s7.get("/graph/person/{name}/recommendations")
def get_recommendations(name: str) -> list[dict]:
    """Get friend recommendations for a person.

    Recommend friends-of-friends who are NOT already direct friends.
    Return them sorted by number of mutual friends (descending).
    If person not found, return 404.

    Each result should include: name, city, mutual_friends (count).
    """
    # TODO: Connect to Neo4J}
    driver=GraphDatabase.driver(settings.neo4j_url, auth=(settings.neo4j_user, settings.neo4j_password))

    # TODO: First check if person exists, return 404 if not
    with driver.session() as session:
        exists=session.run("MATCH (p:Person {name:$name}) RETURN p",
                           name=name).single()
        if not exists:
            raise HTTPException(status_code=404, detail=f"Person '{name}' not found ")
    # TODO: Find friends-of-friends not already friends
        result=session.run(
        """MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(f:Person)-[:FRIENDS_WITH]-(rec:Person)
        WHERE NOT (p)-[:FRIENDS_WITH]-(rec) AND p <> rec
        RETURN rec.name AS name, rec.city AS city, COUNT(DISTINCT f) AS mutual_friends
        ORDER BY mutual_friends DESC""",
        name=name
    )
    # TODO: Count mutual friends and sort descending
        return [{'name':record['name'],
             'city':record['city'],
             'mutual_friends':record['mutual_friends'],
             }
             for record in result]
