-- Once we have imported all of the subawards, we can mark them as imported.



update broker_subaward set imported = true where imported is false;
