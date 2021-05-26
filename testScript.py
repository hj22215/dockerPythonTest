#!/usr/bin/env python
# coding: utf-8

# In[1]:


from cust_objs import fin_data


# In[2]:


db = fin_data()
print(len(db.select("""select * from tblLogData order by logDate desc limit 500""") ))
db.close()


# In[1]:


#!jupyter nbconvert --to script testScript.ipynb


# In[ ]:




