Data_scientia
==============================

The project
==============================
In this project you can find a pipeline to train and 
deploy a model for predict the hospital occupancy in 
the Ciudad de México.


Getting the data
==============================
In the folder data_scientia/data, execute the scripts 
capacidad_hospitalaria.py and covid_municipalities.py. 
These to scripts downloaded the daily status of hospital 
occupancy and the daily cases of covid cases in Ciudad 
de México.

#Creating features
==============================
In the folder data_scientis/features execute the script
train_data.py. These create the training dataset calculating
differents time series features using the municipality covid
cases near from the differents hospitals. 

#Train the model
==============================
In the notebook experiment.ipynb, you can see the pipeline
to train, validate, visualize and see the feature
importance of the train model.

#Deploy the model
==============================
To deploy a new model, you have to be positioned in the 
chalice folder and execute: `chalice deploy --connection-timeout 500 `

#Notebooks
==============================


Data Mexico
