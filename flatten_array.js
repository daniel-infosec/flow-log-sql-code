if (A === undefined) {
    return undefined;
 }
 var ret_obj = {};
 for (var i = 0; i < A.length; i++) {
       var obj = A[i];
       if ((KEY_NAME in obj) && (VALUE_NAME in obj)) {
          ret_obj[obj[KEY_NAME]] = obj[VALUE_NAME];
       }
  }
 return ret_obj;
