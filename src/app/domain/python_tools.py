
import inspect

def get_current_callable():
    current_frame = inspect.currentframe()
    try:
        caller_frame = current_frame.f_back  # Get the frame of the caller
        caller_code = caller_frame.f_code  # Get the code object of the caller
        function_name = caller_code.co_name  # Get the function name
        
        # Get the caller's local variables
        caller_locals = caller_frame.f_locals

        # Check if 'self' is in locals to identify if it's a method of a class
        if 'self' in caller_locals:
            instance = caller_locals['self']
            return getattr(instance, function_name)
        else:
            return globals().get(function_name)
    finally:
        del current_frame
        