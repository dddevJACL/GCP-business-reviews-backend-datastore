from flask import Flask, request
from google.cloud import datastore

BUSINESSES = "businesses"
BUSINESSES_ATTRIBUTES = ["owner_id", "name", "street_address", "city", "state", "zip_code"]
REVIEWS = "reviews"
POST_PUT_ERROR = {"Error" : "The request body is missing at least one of the required attributes"}

app = Flask(__name__)
client = datastore.Client()

@app.route('/')
def index():
    """
    TODO
    """
    return 'Please navigate to /' + BUSINESSES + "or /" + REVIEWS + "to use this API"


def get_id_error(entity_type):
    """
    Returns a generic error message for when the id for a given entity type is not found
    """
    id_error_messages = {
        "businesses": {"Error": "No business with this business_id exists"},
        "reviews": {"Error": "No review with this review_id exists"}
    }
    return id_error_messages[entity_type]


@app.route('/' + BUSINESSES, methods=['GET'])
def get_all_businesses():
    """
    TODO
    """
    datastore_query = client.query(kind=BUSINESSES)
    query_results = list(datastore_query.fetch())
    query_results = add_id_attribute(query_results)
    return query_results

def add_id_attribute(datastore_entities_list, more_than_one=True):
    """
    Takes a list of datastore entities, and adds an 'id' attribute to them
    based on the datastore key.id value. If more_than_one is True, this function
    returns a list, else it returns the lone entity
    """
    for entity in datastore_entities_list:
        entity["id"] = entity.key.id
    if more_than_one:
        return datastore_entities_list
    return datastore_entities_list[0]


@app.route('/' + BUSINESSES + '/<int:id>', methods=['GET'])
def get_business(id):
    """
    TODO
    """
    requested_business = get_entity_by_id(id, BUSINESSES)
    if requested_business is None:
        return (get_id_error(BUSINESSES), 404)
    return requested_business


def get_entity_by_id(id, entity_type):
    """
    TODO
    """
    entity_key = client.key(entity_type, id)
    entity = client.get(key=entity_key)
    if entity is None:
        return None
    entity = add_id_attribute([entity], False)
    return entity


@app.route('/' + BUSINESSES, methods=['POST'])
def post_business():
    """
    TODO
    """
    body_content = request.get_json()
    valid_request = validate_post(body_content, BUSINESSES_ATTRIBUTES)

    if valid_request:
        return create_entity(body_content, BUSINESSES, BUSINESSES_ATTRIBUTES)
    
    return (POST_PUT_ERROR, 400)


def create_entity(json_request, entity_type, entity_attributes):
    """
    TODO
    """
    new_entity = datastore.Entity(key=client.key(entity_type))
    new_entity.update(create_post_dict(json_request, entity_attributes))
    client.put(new_entity)
    new_entity = add_id_attribute([new_entity], False)
    return (new_entity, 201)


def validate_post(json_request, arr_of_keys):
    """
    Checks to see if required attributes are included in body of Post request.
    Expects to recieve and array of attribute keys to test on the json body.
    Returns True if the json request is valid, or False otherwise.
    """
    for key in arr_of_keys:
        if key not in json_request:
            return False
    return True


def create_post_dict(json_request, arr_of_keys):
    """
    Creates and returns dictionary for use in a datastore.Entity update call, 
    usingThe given json_request and array of key names.
    """
    # WILL THIS FUNCTION CAUSE A PROBLEM???? CAN IT BE A DICT, OR DOES IT NEED TO BE WRITTEN
    # IN UPDATE FUNCTION CALL???
    update_dict = dict()
    for key in arr_of_keys:
        update_dict[key] = json_request[key]
    return update_dict


@app.route('/' + BUSINESSES + '/<int:id>', methods=['PUT'])
def edit_business(id):
    """
    TODO
    """
    requested_business = get_entity_by_id(id, BUSINESSES)
    if requested_business is None:
        return (get_id_error(BUSINESSES), 404)
    
    body_content = request.get_json()
    valid_request = validate_post(body_content, BUSINESSES_ATTRIBUTES)

    if valid_request:
        return edit_entity(body_content, BUSINESSES_ATTRIBUTES)
    
    return (POST_PUT_ERROR, 400)


def edit_entity(json_request, entity_attributes):
    """
    TODO
    """
    update_entity.update(create_post_dict(json_request, entity_attributes))
    client.put(update_entity)
    update_entity = add_id_attribute([update_entity], False)
    return (update_entity, 200)


@app.route('/' + BUSINESSES + '/<int:id>', methods=['DELETE'])
def delete_business(id):
    """
    TODO
    """
    requested_business = get_entity_by_id(id, BUSINESSES)
    if requested_business is None:
        return (get_id_error(BUSINESSES), 404)
    
    entities_to_delete = [(id, BUSINESSES)]

    query = client.query(kind=REVIEWS)
    query.add_filter("business_id", "==", int(id))
    query_results = list(query.fetch())
    for result in query_results:
        result['id'] = result.key.id
        entities_to_delete.append((result['id'], REVIEWS))
    
    return delete_entities_from_list(entities_to_delete)
    
    
def delete_entities_from_list(entity_list):
    """
    Deletes all entities from the entity list from datastore.
    entity_list is a list of tuples, where the first value is the id of an entity,
    and the second value is the entity type (ie. a review or a business)
    IMPORTANT: This function assumes that ids passed to the list are valid
    """
    for entity in entity_list:
        entity_key = client.key(entity[1], entity[0])
        client.delete(entity_key)
    return ('', 204)











@app.route('/' + REVIEWS + '/<int:id>', methods=['DELETE'])
def delete_review(id):
    """
    TODO
    """
    requested_review = get_entity_by_id(id, REVIEWS)
    if requested_review is None:
        return (get_id_error(REVIEWS), 404)
    return delete_entities_from_list([(id, REVIEWS)])


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
