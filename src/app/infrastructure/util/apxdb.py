""" APXDB-specific utils """

def wrap_in_session_procs(query) -> str:
    return f"""
------------------------------------------------------------------------------------------------------------
DECLARE @sessionGUID uniqueidentifier = NULL
EXEC APXFirm.APX.pAPXSessionCreate @userID = -1001, @configurationID = 4, @sessionGUID = @sessionGUID OUT
EXEC APXFirm.Apxuser.pSessionInfoSetGuid @sessionGUID = @sessionGUID
------------------------------------------------------------------------------------------------------------

{query}

------------------------------------------------------------------------------------------------------------
EXEC APXFirm.APX.pAPXSessionDelete @sessionGUID = @sessionGUID
------------------------------------------------------------------------------------------------------------
"""



