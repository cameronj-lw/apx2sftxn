
# core python
import json
import logging
import numpy as np
from typing import List, Optional, Union, Tuple
import xml.etree.ElementTree as ET

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, float) and np.isnan(obj):
            return None
        return super(CustomJSONEncoder, self).default(obj)


class DefaultRESTFormatter:
    # TODO: Create a multi-repo post formatter, e.g. for PriceByIMEX, 
    # to provide a full summary of inserts to mutliple repo's

    def success_get(self, data: Union[dict, list], message: Union[str,None]=None, status: str='success') -> Tuple[dict, int]:
        # Recursively replace NaN values with None
        clean_data = self._replace_nan_with_null(data)
        
        res = ({
            'data': clean_data,
            'message': message,
            'status': status,
        }, 200)
        logging.info(f'DefaultRESTFormatter success_get returning {len(data)} data items')  # TODO_CLEANUP: too verbose logging
        # logging.info(res)
        return res

    def get_repo_str(self, repo_classes):
        # Build repo str, if any readable repo name(s) can be found
        repo_str = '.'  # default, unless it gets overriden below:
        if isinstance(repo_classes, type):
            if hasattr(repo_classes, 'readable_name'):
                repo_str = ': ' + repo_classes.readable_name()
        elif isinstance(repo_classes, list):
            # Need to first gather all the names, so we can comma-join them after
            repo_names = []
            for c in repo_classes:
                if hasattr(c, 'readable_name'):
                    repo_names.append(c.readable_name())
            # Now comma-join them
            if len(repo_names):
                repo_str = ': ' + ', '.join(repo_names)
        return repo_str

    def success_post(self, row_cnt, repo_classes: Union[type, list, None]=None, msg: str=None) -> Tuple[dict, int]: 

        # Get repo str
        repo_str = self.get_repo_str(repo_classes)

        # Build msg and log it
        if not msg:
            if row_cnt:
                msg = f"Successfully saved {row_cnt} row{'' if row_cnt == 1 else 's'}{repo_str}"
            else:
                msg = f"Succeeded, but nothing was saved{repo_str}"
        logging.info(msg)

        # Return complete results
        if row_cnt:
            return {
                'data': None,
                'message': msg,
                'status': 'success',
            }, 201
        else:
            return {
                'data': None,
                'message': msg,
                'status': 'warning',
            }, 200

    def success_delete(self, row_cnt: int) -> Tuple[dict, int]:
        if row_cnt:
            return {
                'data': None,
                'message': f"Successfully deleted {row_cnt} row{'' if row_cnt == 1 else 's'}.",
                'status': 'success',
            }, 201
        else:
            return {
                'data': None,
                'message': f"Succeeded, but found nothing to delete.",
                'status': 'warning',
            }, 200

    def exception(self, e: Exception, http_return_code: int=500) -> Tuple[dict, int]:
        logging.exception(f'Returning {http_return_code} due to {type(e).__name__}: {e}')
        return {
            'data': f'{type(e).__name__}: {str(e)}',
            'message': f'{type(e).__name__}: {str(e)}',
            'status': 'error', 
        }, http_return_code

    def _replace_nan_with_null(self, obj):
        if isinstance(obj, float) and np.isnan(obj):
            return None
        elif isinstance(obj, dict):
            return {k: self._replace_nan_with_null(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._replace_nan_with_null(item) for item in obj]
        else:
            return obj


class DefaultXMLFormatter:
    """ Format output to XML (rather than JSON) """
    
    def success_get(self, data: Union[dict, list], message: Union[str, None] = None, status: str = 'success') -> Tuple[str, int]:
        # Recursively replace NaN values with None
        clean_data = self._replace_nan_with_null(data)
        
        # Convert to XML
        root = ET.Element("response")
        ET.SubElement(root, "status").text = status
        ET.SubElement(root, "message").text = message if message else ""
        
        data_element = ET.SubElement(root, "data")
        self._dict_to_xml(data_element, clean_data)
        
        # Generate XML string
        xml_str = ET.tostring(root, encoding="unicode")
        logging.info(f'DefaultXMLFormatter success_get returning data in XML format')
        
        return xml_str, 200

    def success_post(self, row_cnt, repo_classes: Union[type, list, None] = None, msg: str = None) -> Tuple[str, int]:
        root = ET.Element("response")
        status = 'success' if row_cnt else 'warning'
        ET.SubElement(root, "status").text = status
        
        # Get repo str
        repo_str = self.get_repo_str(repo_classes)
        
        # Build message
        if not msg:
            if row_cnt:
                msg = f"Successfully saved {row_cnt} row{'' if row_cnt == 1 else 's'}{repo_str}"
            else:
                msg = f"Succeeded, but nothing was saved{repo_str}"
        logging.info(msg)
        
        ET.SubElement(root, "message").text = msg
        ET.SubElement(root, "data").text = ""
        
        xml_str = ET.tostring(root, encoding="unicode")
        return xml_str, 201 if row_cnt else 200

    def success_delete(self, row_cnt: int) -> Tuple[str, int]:
        root = ET.Element("response")
        status = 'success' if row_cnt else 'warning'
        ET.SubElement(root, "status").text = status
        
        msg = f"Successfully deleted {row_cnt} row{'' if row_cnt == 1 else 's'}." if row_cnt else "Succeeded, but found nothing to delete."
        logging.info(msg)
        
        ET.SubElement(root, "message").text = msg
        ET.SubElement(root, "data").text = ""
        
        xml_str = ET.tostring(root, encoding="unicode")
        return xml_str, 201 if row_cnt else 200

    def exception(self, e: Exception, http_return_code: int = 500) -> Tuple[str, int]:
        root = ET.Element("response")
        ET.SubElement(root, "status").text = "error"
        ET.SubElement(root, "message").text = f'{type(e).__name__}: {str(e)}'
        ET.SubElement(root, "data").text = f'{type(e).__name__}: {str(e)}'
        
        xml_str = ET.tostring(root, encoding="unicode")
        logging.exception(f'Returning {http_return_code} due to {type(e).__name__}: {e}')
        return xml_str, http_return_code

    def _replace_nan_with_null(self, obj):
        if isinstance(obj, float) and np.isnan(obj):
            return None
        elif isinstance(obj, dict):
            return {k: self._replace_nan_with_null(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._replace_nan_with_null(item) for item in obj]
        else:
            return obj

    def _dict_to_xml(self, parent, data):
        if isinstance(data, dict):
            for key, value in data.items():
                child = ET.SubElement(parent, key)
                self._dict_to_xml(child, value)
        elif isinstance(data, list):
            for item in data:
                item_element = ET.SubElement(parent, "item")
                self._dict_to_xml(item_element, item)
        else:
            parent.text = str(data)

    def get_repo_str(self, repo_classes):
        repo_str = '.'
        if isinstance(repo_classes, type):
            if hasattr(repo_classes, 'readable_name'):
                repo_str = ': ' + repo_classes.readable_name()
        elif isinstance(repo_classes, list):
            repo_names = [c.readable_name() for c in repo_classes if hasattr(c, 'readable_name')]
            if repo_names:
                repo_str = ': ' + ', '.join(repo_names)
        return repo_str

