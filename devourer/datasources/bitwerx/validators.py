import logging
from jsonschema import validate, ValidationError

logger = logging.getLogger('devourer.datasource.bitwerx')


def validate_line_item(data):

    schema = {
        "definitions": {
            # "targetParasiteItem": {
            #     "type": "object",
            #     "properties": {
            #         "Species": {"type": "string"}
            #     },
            #     "required": [
            #         "Species"
            #     ]
            # },
            # "marketedSpeciesItem": {
            #     "type": "object",
            #     "properties": {
            #         "Species": {"type": "string"},
            #         "MaximumWeightInPounds": {"type": "string"},
            #         "MinimumAgeInDays": {"type": "null"},
            #         "MinimumWeightInPounds": {"type": "string"}
            #     },
            #     "required": [
            #         "Company"
            #     ]
            # },
            # "marketedByCompanyItem": {
            #     "type": "object",
            #     "properties": {
            #         "Company": {"type": "string"}
            #     },
            #     "required": [
            #         "Company"
            #     ]
            # },
            # "disciplineItem": {
            #     "type": "object",
            #     "properties": {
            #         "Discipline": {"type": "string"}
            #     },
            #     "required": [
            #         "Discipline"
            #     ]
            # },
            # "dosageFormItem": {
            #     "type": "object",
            #     "properties": {
            #         "DosageForm": {"type": "string"}
            #     },
            #     "required": [
            #         "DosageForm"
            #     ]
            # },
            # "activeIngredientItem": {
            #     "type": "object",
            #     "properties": {
            #         "ActiveIngredient": {"type": "string"},
            #         "ActiveIngredientAmountInMilligrams": {"type": "string"},
            #         "ActiveIngredientAmountPercentage": {"type": "null"}
            #     },
            #     "required": [
            #         "ActiveIngredient",
            #         "ActiveIngredientAmountInMilligrams",
            #         "ActiveIngredientAmountPercentage"
            #     ]
            # },
            "taxonomyValuesObj": {
                "type": "object",
                "properties": {
                    # "AffectedAnatomicalRegions": {"type": "array"},
                    # "AffectedBodySystems": {"type": "array"},
                    # "ActiveIngredients": {
                    #     "type": "array",
                    #     "items": {
                    #         "$ref": "#/definitions/activeIngredientItem"
                    #     }
                    # },
                    # "Brand": {"type": "string"},
                    # "DaysOfTreatment": {"type": "string"},
                    # "Discontinued": {"type": "string"},
                    # "Disciplines": {
                    #     "type": "array",
                    #     "items": {
                    #         "$ref": "#/definitions/disciplineItem"
                    #     }
                    # },
                    # "DosageForms": {
                    #     "type": "array",
                    #     "items": {
                    #         "$ref": "#/definitions/dosageFormItem"
                    #     }
                    # },
                    # "Flavors": {"type": "array"},
                    # "FoodForms": {"type": "array"},
                    # "Indications": {"type": "array"},
                    # "Inventory": {"type": "string"},
                    # "InventoryType": {"type": "string"},
                    # "LifeStage": {"type": "null"},
                    # "MarketedByCompanies": {
                    #     "type": "array",
                    #     "items": {
                    #         "$ref": "#/definitions/marketedByCompanyItem"
                    #     }
                    # },
                    # "MarketedSpecies": {
                    #     "type": "array",
                    #     "items": {
                    #         "$ref": "#/definitions/marketedSpeciesItem"
                    #     }
                    # },
                    "marketerSku": {"type": ["string", "null"]},
                    # "PackagedProduct": {"type": "string"},
                    # "PackagingCount": {"type": "string"},
                    # "PrescriptionRequired": {"type": "string"},
                    # "PharmaceuticalAgent": {"type": "null"},
                    # "Product": {"type": "string"},
                    # "RevenueType": {"type": "string"},
                    # "RouteOfAdministration": {"type": "string"},
                    # "Procedure": {"type": "null"},
                    # "Service": {"type": "null"},
                    # "ServiceType": {"type": "null"},
                    # "TargetBacteria": {"type": "array"},
                    # "TargetParasites": {
                    #     "type": "array",
                    #     "items": {
                    #         "$ref": "#/definitions/targetParasiteItem"
                    #     }
                    # },
                    # "TargetViruses": {"type": "array"},
                    # "UsDrugSchedule": {"type": "null"},
                    # "VolumeInMilliliters": {"type": "null"},
                    "weightInOunces": {"type": ["string", "null"]},
                    "weightInPounds": {"type": ["string", "null"]},
                },
                "required": [
                    # "AffectedAnatomicalRegions",
                    # "AffectedBodySystems",
                    # "ActiveIngredients",
                    # "Brand",
                    # "DaysOfTreatment",
                    # "Discontinued",
                    # "Disciplines",
                    # "DosageForms",
                    # "Flavors",
                    # "FoodForms",
                    # "Indications",
                    # "Inventory",
                    # "InventoryType",
                    # "LifeStage",
                    # "MarketedByCompanies",
                    # "MarketedSpecies",
                    "marketerSku",
                    # "PackagedProduct",
                    # "PackagingCount",
                    # "PrescriptionRequired",
                    # "PharmaceuticalAgent",
                    # "Product",
                    # "RevenueType",
                    # "RouteOfAdministration",
                    # "Procedure",
                    # "Service",
                    # "ServiceType",
                    # "TargetBacteria",
                    # "TargetParasites",
                    # "TargetViruses",
                    # "UsDrugSchedule",
                    # "VolumeInMilliliters",
                    "weightInOunces",
                    "weightInPounds",
                ]
            },
            "crossWalkMappingItem": {
                "type": "object",
                "properties": {
                    "CrosswalkType": {"type": "string"},
                    # "AccountLabel": {"type": "string"},
                    # "AccountNumber": {"type": "string"}
                },
                "required": [
                    "CrosswalkType",
                    # "AccountLabel"
                ]
            },
            "taxonomyMappingItem": {
                "type": "object",
                "properties": {
                        # "taxonomyNodeType": {"type": "string"},
                        "taxonomyNodeLabel": {"type": "string"},
                        "taxonomyValues": {"$ref": "#/definitions/taxonomyValuesObj"},

                },
                "required": [
                    # "taxonomyNodeType",
                    "taxonomyNodeLabel",
                    "taxonomyValues"
                ]
            },
            # "supplementalMappingItem": {
            #     "type": "object",
            #     "properties": {
            #         "isCompounded": {"type": "boolean"},
            #         "isEmergency": {"type": "boolean"},
            #         "isOnlineSale": {"type": "boolean"}
            #     },
            #     "required": [
            #         "isCompounded",
            #         "isEmergency",
            #         "isOnlineSale"
            #     ]
            # },
            "mappingItem": {
                "type": "object",
                "properties": {
                    "crossWalkMapping": {
                        "type": "array",
                        "items": {
                            "$ref": "#/definitions/crossWalkMappingItem"
                        }
                    },
                    "taxonomyMapping": {
                        "type": "array",
                        "items": {
                            "$ref": "#/definitions/taxonomyMappingItem"
                        }
                    },
                    # "supplementalMapping": {
                    #     "type": "array",
                    #     "items": {
                    #         "$ref": "#/definitions/supplementalMappingItem"
                    #     }
                    # },
                },
                "required": [
                    # "crossWalkMapping",
                    "taxonomyMapping",
                    # "supplementalMapping"
                ]
            },
        },
        "type": "object",
        "properties": {
            "lineItemId": {"type": "string"},
            # "IsActive": {"type": "number"},
            "isDeleted": {"type": "boolean"},
            # "SiteId": {"type": "string"},
            "updated": {"type": "string"},
            "created": {"type": "string"},
            "clientId": {"type": "string"},
            "patientId": {"type": "string"},
            "transactionDate": {"type": "string"},
            "description": {"type": "string"},
            "quantity": {"type": "string"},
            "lineAmount": {"type": "string"},
            "isVoided": {"type": "boolean"},
            "invoiceId": {"type": "string"},
            # "ItemId": {"type": "string"},
            # "ResourceId": {"type": "string"},
            "mappings": {
                "type": "array",
                "items": {
                    "$ref": "#/definitions/mappingItem"
                }
            }
        },
        "required": [
            "lineItemId",
            "isDeleted",
            "updated",
            "created",
            "clientId",
            "patientId",
            "transactionDate",
            "description",
            "quantity",
            "lineAmount",
            "isVoided",
            "invoiceId",
        ]
    }

    success = True
    try:
        validate(instance=data, schema=schema)
    except ValidationError as exc:
        success = False
        logging.error(exc)

    return success
