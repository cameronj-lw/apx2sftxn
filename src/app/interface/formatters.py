
# core python
import logging
from typing import List, Optional, Union, Tuple

class DefaultRESTFormatter:
    # TODO: Create a multi-repo post formatter, e.g. for PriceByIMEX, 
    # to provide a full summary of inserts to mutliple repo's

    def success_get(self, data: Union[dict, list], message: Union[str,None]=None, status: str='success') -> Tuple[dict, int]:
        res = ({
            'data': data,
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

