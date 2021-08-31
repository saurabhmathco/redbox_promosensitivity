#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys

get_ipython().system('{sys.executable} -m pip install keyring artifacts-keyring')
get_ipython().system('{sys.executable} -m pip install --pre --upgrade --trusted-host pkgs.dev.azure.com --trusted-host pypi.org --trusted-host "*.blob.core.windows.net" --trusted-host files.pythonhosted.org --extra-index-url https://pkgs.dev.azure.com/mathco-products/_packaging/pip-codex-wf%40Local/pypi/simple/ "codex-widget-factory<=0.1"')


# In[1]:


# tags to identify this iteration when submitted
# example: codex_tags = {'env': 'dev', 'region': 'USA', 'product_category': 'A'}

codex_tags = {
}

from codex_widget_factory import utils
results_json=[]


# ### Ingestion Transaction

# In[ ]:


#BEGIN WIDGET CODE BELOW...

from codex_widget_factory.ingestion.file_system import get_ingested_data as ingestion_file_system_get_ingested_data
response_0 = ingestion_file_system_get_ingested_data(file_path='transaction_details_internal_201901041519.csv',
datasource_type='azure_blob_storage',
connection_uri="DefaultEndpointsProtocol=https;AccountName=coach;AccountKey=qtMAw9Z1T+1oOl+joMwfzdvnR0exMA4Qw50vniiaXOFvpOeFG7TP+g+DP4/iU7VKAUhirSpzrESvjF3U2Ld0KA==;EndpointSuffix=core.windows.net",
container_name='ddrs11/redbox_surabhi')
results_json.append({
  'type': 'Ingestion',
  'name': 'Transaction',
  'component': 'get_ingested_data',
  'visual_results': utils.get_response_visuals(response_0),
  'metrics': False
})
utils.render_response(response_0)

#END WIDGET CODE


# ### Custom Filter Unknown Promotion code

# In[ ]:


#BEGIN CUSTOM CODE BELOW...

#upstream params found
# response_0


trans_df=response_0[["promotioncampaignid","promotioncode","promoamount"]]
trans_df.sort_values("promoamount",inplace=True)

trans_df["promotioncode"][trans_df["promoamount"]==0]="No Promo"

trans_df=trans_df[trans_df["promoamount"]>0]
trans_df.reset_index()
# promomode=trans_df["promotioncampaignid"].mode()
# print(promomode)
# trans_df[trans_df['promotioncampaignid'] == promomode]

# for i in range(len(trans_df)):
#     if trans_df["promotioncode"].iloc[i]=="Unknown"
#         if trans_df["promotioncampaignid"].iloc[i]==promomode:
print(trans_df["promotioncode"][trans_df["promotioncode"]=="Unknown"].count())           

# trans_df["promotioncode"][trans_df["promotioncampaignid"]==promomode]
# trans_df["promotioncode"][trans_df["promotioncode"]=="Unknown"]=promomode
# promotionmode

#put your output in this response param for connecting to downstream widgets
response_1=trans_df

#END CUSTOM CODE


# In[ ]:


trans_df['rentaldate'] = pd.to_datetime(trans_df['rentaldate'])
                                   
trans_df['new_date_column'] = trans_df['rentaldate'].dt.date


# In[ ]:


trans_df["month"]=1

import datetime
start_date = pd.to_datetime("2018-02-01").date()
end_date = pd.to_datetime("2018-05-01").date()
day_delta=datetime.timedelta(days=1)
count=0

for i in range(0,(end_date - start_date).days,30):
    start_i=(start_date+i*day_delta)
    end_i=(start_date+(i+30)*day_delta)
    x=trans4_df['new_date_column'].loc[(trans4_df['new_date_column'] >= start_i) & (trans4_df['new_date_column'] < end_i)]
    y=trans4_df.count_transactionid.iloc[i:i+30]


# ### Please save and checkpoint notebook before submitting params

# In[ ]:



currentNotebook = 'Imputing_promocode_202108240549.ipynb'

get_ipython().system('jupyter nbconvert --to script {currentNotebook} ')


# In[ ]:



utils.submit_config_params(url='https://codex-api-stage.azurewebsites.net/codex-api/projects/upload-config-params/F57A0067994B9211477396CD54915E30795DA1A9C96EA227E7ABE4076AFD6E8A', nb_name=currentNotebook, results=results_json, codex_tags=codex_tags, args={})


# In[ ]:




