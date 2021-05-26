#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from cust_objs import fin_data


# In[ ]:


db = fin_data()
print(len(db.select("""select * from tblLogData order by logDate desc limit 500""") ))
db.close()


# In[1]:


#!jupyter nbconvert --to script testScript.ipynb


# In[ ]:




