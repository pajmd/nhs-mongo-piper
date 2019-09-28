from app.search_engine.solrclient.solrcommands import SolrOp as op


class CreateCollectionException(Exception):
    pass


class DeleteCollectionException(Exception):
    pass


class AddFieldSchemaException(Exception):
    pass


class IndexingFileException(Exception):
    pass


def get_error(operation_type, operation, response):
    if operation_type == op.ADMIN:
        return "Error %s - %s: %s" % (operation_type.name, operation.name, response["exception"]["msg"])
    elif operation_type == op.SCHEMA:
        # err = {
        #     "details": [
        #         {
        #         "add-field": {
        #             "name": "medecine",
        #             "type": "text_general",
        #             "multiValued": False,
        #             "stored": True
        #         },
        #         "errorMessages": ["Field 'medecine' already exists.\n"]
        #         }
        #     ]
        # }
        def extract_error(errors):
            error = errors[0]
            for k, v in error.items():
                if k == 'errorMessages':
                    errmsg = error[k]
                else:
                    opr = k
            return opr, errmsg
        details = response["error"]["details"]
        return "Error %s %s: %s" % (operation_type.name, *extract_error(details))
    elif operation_type == op.INDEXING:
        return "Error %s - %s: %s" % (operation_type.name, operation.name, response['error']['msg'])
    else:
        return "Error %s - %s: %s" % (operation_type.name, operation.name, response)


def raise_for_status(operation_type, operation, resp):
    """
    reaises an exception at the first erro found
    :param operation_type:
    :param operation:
    :param resp: a single or a list of requests.Response()
    :return:
    """
    if isinstance(resp, list):
        for r in resp:
            inspect(operation_type, operation, r)
    else:
        inspect(operation_type, operation, resp)


def inspect(operation_type, operation, resp):
    print("%s - %s: %s" % (operation_type.name, operation.name, resp))
    if resp.status_code != 200:
        response = resp.json()
        # response = json.loads(resp.text)
        if operation_type == op.ADMIN:
            if response["responseHeader"]["status"] != 0:
                if operation == op.CREATE_COLLECTION:
                    raise CreateCollectionException(get_error(operation_type, operation, response))
                elif operation == op.DELETE_COLLECTION:
                    raise DeleteCollectionException(get_error(operation_type, operation, response))
        elif operation_type == op.SCHEMA:
            if response["responseHeader"]["status"] != 0:
                if operation == op.ADD_FIELD:
                    raise AddFieldSchemaException(get_error(operation_type, operation, response))
        elif operation_type == op.INDEXING:
            if response["responseHeader"]["status"] != 0:
                if operation == op.ADD_FILE or operation == op.ADD_DOCUMENTS:
                    raise IndexingFileException(get_error(operation_type, operation, response))
