# Name: Daniel Dalinda
# OSU Email: dalindad@oregonstate.edu
# Github: dddevJACL
# Course: CS493 - Cloud Application Development
# Assignment: Assignment 2 - REST API Implementation with GAE and Cloud Datastore
# Due Date: April 22nd, 2024
# (Submitted late): April 23rd, 2024
# Description: A REST API allowing for CRUD for Business and Review entities. This API uses Flask, GAE and
#              Cloud Datastore.
#              ******************************************************************************************
#              SOURCES CITED: CS493 Modules 2 and 3. Google Cloud 'Building a Python3 App on App Engine'
#                             Tutorial. (The same one that was used for Assignment 1)
#              *******************************************************************************************


from flask import Flask, request
from google.cloud import datastore

BUSINESSES = "businesses"
BUSINESSES_REQUIRED_ATTRIBUTES = ["owner_id", "name", "street_address", "city", "state", "zip_code"]
REVIEWS = "reviews"
REVIEWS_REQUIRED_ATTRIBUTES = ["user_id", "business_id", "stars"]
POST_PUT_ERROR = {"Error" : "The request body is missing at least one of the required attributes"}

app = Flask(__name__)
client = datastore.Client()

@app.route('/')
def index():
    """
    The main route of the API. Tells users to navigate to relevant url
    """
    return 'Please navigate to /' + BUSINESSES + " or /" + REVIEWS + " to use this API"


def get_id_error(entity_type):
    """
    Returns a generic error message for when the id for a given entity type is not found
    """
    id_error_messages = {
        "businesses": {"Error": "No business with this business_id exists"},
        "reviews": {"Error": "No review with this review_id exists"}
    }
    return (id_error_messages[entity_type], 404)


@app.route('/' + BUSINESSES, methods=['GET'])
def get_all_businesses():
    """
    Returns a list of all businesses
    """
    return create_query_list(BUSINESSES)


def create_query_list(entity_type, filter=None):
    """
    Takes an entity type (review or business), and optionally takes a filter type.
    It then queries the datastore with the given arguments. 
    If the returned query list is not none, an id attribute is also added to each
    entity in the datastore_query_results list
    """
    datastore_query = client.query(kind=entity_type)
    if filter:
        datastore_query.add_filter(filter[0], filter[1], filter[2])
    datastore_query_results = list(datastore_query.fetch())
    if len(datastore_query_results) >= 1:
        datastore_query_results = add_id_attribute(datastore_query_results)
    return datastore_query_results


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
    Returns the business with the given id if it exists. If not, it
    calls the get_id_error function, which will return 404 with an appropriate message.
    """
    requested_business = get_entity_by_id(id, BUSINESSES)
    if requested_business is None:
        return get_id_error(BUSINESSES)
    return requested_business


def get_entity_by_id(id, entity_type):
    """
    A generic function for searching the datastore for an id of a particular entity type.
    If the entity is found, an id attribute is added to the entity and returned.
    Otherwise, this function returns None.
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
    Creates a business in the datastore if the request sent is valid, otherwise,
    a 400 error is returned.
    """
    body_content = request.get_json()
    valid_request = validate_post(body_content, BUSINESSES_REQUIRED_ATTRIBUTES)
    if valid_request:
        return create_entity(body_content, BUSINESSES, BUSINESSES_REQUIRED_ATTRIBUTES)
    return (POST_PUT_ERROR, 400)


def create_entity(json_request, entity_type, entity_attributes):
    """
    A generic function for creating entities based on the provided json request.
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
    Creates and returns dictionary for use in datastore. Calls entity update, 
    using the given json_request and array of key names, which holds the required
    attributes of the given entity.
    """
    update_dict = dict()
    for key in arr_of_keys:
        update_dict[key] = json_request[key]
    if "review_text" in json_request:
        update_dict["review_text"] = json_request["review_text"]
    return update_dict


@app.route('/' + BUSINESSES + '/<int:id>', methods=['PUT'])
def edit_business(id):
    """
    Edit a business with the the given id with the provided request,
    if the business exists. If not, a 400 error is returned along with an appropriate
    message.
    """
    requested_business = get_entity_by_id(id, BUSINESSES)
    if requested_business is None:
        return get_id_error(BUSINESSES)
    body_content = request.get_json()
    valid_request = validate_post(body_content, BUSINESSES_REQUIRED_ATTRIBUTES)
    if valid_request:
        return edit_entity(requested_business, body_content, BUSINESSES_REQUIRED_ATTRIBUTES)   
    return (POST_PUT_ERROR, 400)


def edit_entity(entity, json_request, entity_attributes):
    """
    Edits an entity with the provided json request body, by calling the
    create post dict function. This function also adds an id attribute for 
    returning. Finally, this function returns the entity information, along with
    a response code of 200. This function assumes that the existence of the entity
    to be updated has already been validated.
    """
    update_entity = entity
    update_entity.update(create_post_dict(json_request, entity_attributes))
    client.put(update_entity)
    update_entity = add_id_attribute([update_entity], False)
    return (update_entity, 200)


@app.route('/' + BUSINESSES + '/<int:id>', methods=['DELETE'])
def delete_business(id):
    """
    Deletes a business with the given id, if it exists. If it does exist,
    reviews corresponding to this business are also deleted.
    """
    requested_business = get_entity_by_id(id, BUSINESSES)
    if requested_business is None:
        return get_id_error(BUSINESSES)
    entities_to_delete = [(id, BUSINESSES)]
    query_results = create_query_list(REVIEWS, ["business_id", "=", int(id)])
    for result in query_results:
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


@app.route('/owners/<int:id>/' + BUSINESSES, methods=['GET'])
def get_businesses_by_owner(id):
    """
    Gets and returns all businesses associated with the given owner id.
    """
    return create_query_list(BUSINESSES, ["owner_id", "=", int(id)])
    

@app.route('/' + REVIEWS, methods=['POST'])
def post_review():
    """
    Creates a review for a business given that:
        1. The request is valid (all required attributes are included in the request) (else return 400)
        2. The business that is being reviewed exists in datastore (else return 404)
        3. The user has not already created a review for the given business (else return 409)
    If these criteria or met, the review is posted in datastore, and a json response is returned along with a 200 status code.
    """
    body_content = request.get_json()
    valid_request = validate_post(body_content, REVIEWS_REQUIRED_ATTRIBUTES)
    if valid_request:
        business = get_entity_by_id(int(body_content["business_id"]), BUSINESSES)
        if business is None:
            return get_id_error(BUSINESSES)
        user_reviews = create_query_list(REVIEWS, ["user_id", "=", int(body_content["user_id"])])
        for review in user_reviews:
            if review["business_id"] == body_content["business_id"]:
                return ({"Error": "You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review"}, 409)
        return create_entity(body_content, REVIEWS, REVIEWS_REQUIRED_ATTRIBUTES)
    return (POST_PUT_ERROR, 400)


@app.route('/' + REVIEWS + '/<int:id>', methods=['GET'])
def get_review(id):
    """
    Gets and returns a review with the given id, if it exists, else 404 is returned.
    """
    requested_review = get_entity_by_id(id, REVIEWS)
    if requested_review is None:
        return get_id_error(REVIEWS)
    return requested_review


@app.route('/' + REVIEWS + '/<int:id>', methods=['PUT'])
def edit_review(id):
    """
    Edits a review with the given id. If the review doesnt exist already, 404 is returned. If the
    request is invalid, 400 is returned. Else the updated json response is returned along with a status
    code of 200.
    """
    requested_review = get_entity_by_id(id, REVIEWS)
    if requested_review is None:
        return get_id_error(REVIEWS)
    body_content = request.get_json()
    edit_review_required_attributes = ["stars"]
    valid_request = validate_post(body_content, edit_review_required_attributes)
    if valid_request:
        requested_review.update({
            "stars": body_content["stars"]
        })
        if "user_id" in body_content:
            requested_review.update({
            "user_id": body_content["user_id"]
        })
        if "business_id" in body_content:
            requested_review.update({
            "business_id": body_content["business_id"]
        })
        if "review_text" in body_content:
            requested_review.update({
            "review_text": body_content["review_text"]
        })
        client.put(requested_review)
        requested_review["id"] = requested_review.key.id
        return (requested_review, 200)
    return (POST_PUT_ERROR, 400)


@app.route('/' + REVIEWS + '/<int:id>', methods=['DELETE'])
def delete_review(id):
    """
    The review corresponding to the given id is deleted, if it exists.
    If not, 404 is returned.
    """
    requested_review = get_entity_by_id(id, REVIEWS)
    if requested_review is None:
        return get_id_error(REVIEWS)
    return delete_entities_from_list([(id, REVIEWS)])


@app.route('/users/<int:id>/' + REVIEWS, methods=['GET'])
def get_reviews_by_user(id):
    """
    Returns all reviews associated with the given user.
    """
    return create_query_list(REVIEWS, ["user_id", "=", int(id)])


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
