# ALS Recommender

## Description

This machine learning plugin represents the building stage for an Apache Spark ML "Collaborative 
Filtering model". This technique is commonly used for recommender systems and aims to fill in the 
missing entries of a User-Item association matrix. Spark ML uses the ALS (alternating least squares) 
algorithm.

Alternating Least Squares (ALS) matrix factorization, or ALS, attempts to estimate the ratings matrix 
`R` as the product of two lower-rank matrices, `X` and `Y`, i.e. `X * Yt = R`. 

Typically, these approximations are called 'factor' matrices. The general approach is iterative. During 
each iteration, one of the factor matrices is held constant, while the other is solved for using least 
squares. The newly-solved factor matrix is then held constant while solving for the other factor matrix.

This is a blocked implementation of the ALS factorization algorithm that groups the two sets of factors 
(referred to as "users" and "products") into blocks and reduces communication by only sending one copy 
of each user vector to each product block on each iteration, and only for the product blocks that need 
that user's feature vector. This is achieved by pre-computing some information about the "ratings" matrix 
to determine the "out-links" of each user (which blocks of products it will contribute to) and "in-link" 
information for each product (which of the feature vectors it receives from each user block it will depend 
on). This allows us to send only an array of feature vectors between each user block and product block, and 
have the product block find the users' ratings and update the products based on these messages.

For implicit preference data, the algorithm used is based on "Collaborative Filtering for Implicit Feedback 
Datasets", available at http://dx.doi.org/10.1109/ICDM.2008.22, adapted for the blocked approach used here.

Essentially instead of finding the low-rank approximations to the rating matrix `R`, this finds the approximations 
for a preference matrix `P` where the elements of `P` are 1 if r is greater than 0 and 0 if r is less than or equal 
to 0. The ratings then act as "confidence" values related to strength of indicated user preferences rather than 
explicit ratings given to items.

## Configuration
**Reference Name**: Name used to uniquely identify this plugin for lineage, annotating metadata, etc.

### Model Configuration
**Model Name**: The unique name of the machine learning model.

**Model Stage***: The stage of the ML model. Supported values are 'experiment', 'staging', 'production'
and 'archived'. Default is 'experiment'.

### Data Configuration
**User Field**: The name of the input field that defines the user identifiers. The values must be within 
the integer value range.

**Item Field**: The name of the input field that defines the item identifiers. The values must be within 
the integer value range.

**Rating Field**: The name of the input field that defines the item ratings. The values must be within 
the integer value range.

**Data Split**: The split of the dataset into train & test data, e.g. 80:20. Default is 70:30.

### Parameter Configuration
**Factorization Rank**: A positive number that defines the rank of the matrix factorization. Default is 10.

**Nonnegative Constraints**: The indicator to determine whether to apply non negativity constraints for least 
squares. Support values are 'true' and 'false'. Default is 'false'.

**Maximum Iterations**: The maximum number of iterations to train the ALS model. Default is 10.

**Regularization Parameter**: The nonnegative regularization parameter. Default is 0.1.

**User Blocks**: The number of user blocks. Default is 10.

**Item Blocks**: The number of item blocks. Default is 10.

**Implicit Preference**: The indicator to determine whether to use implicit preference. Support values 
are 'true' and 'false'. Default is 'false'.

**Alpha Parameter**: The nonnegative alpha parameter in the implicit preference formulation. Default is 1.0.
