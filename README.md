# GDX Analytics Microservice

[![img](https://img.shields.io/badge/Lifecycle-Maturing-007EC6)](https://github.com/bcgov/repomountie/blob/master/doc/lifecycle-badges.md)


Test veenu
---
The GDX Analytics microservice repository is the working space for the suite of Python based  microservices supporting data retrieval, processing, loading, and other handling as part of our workflow.

## Features

This repository has been structured to support packaging and distrubution. Pipenv is required as a dependency manager, and Python 3.7 to build the virtual environments. Each microservice script is stored in a subdirectory, and each subdirectory contains a README file detailing how to run the microservice. A folder named `lib` provides shared component modules. Under each microservice, `lib` gets installed into the Pipenv as an editable package, and then is imported by its relative path.

## Project Status

This project is currently under development and actively supported by the GDX Analytics Team.

## Contents by Directory:

#### [S3 to Redshift Microservice](./s3_to_redshift)

The S3 to Redshift microservice will read the config `json` to determine the input data location, it's content and how to process that (including column data types, content replacements, datetime formats), and where to output the results (the Redshift table). Each processed file will land in a `<bucket>/processed/` folder in S3, which can be `/processed/good/` or `/processed/bad/` depending on the success or failure of processing the input file. The Redshift `COPY` command is performed as a single transaction which will not commit the changes unless they are successful in the transaction.

#### [CMS Lite Metadata Microservice](./cmslitemetadata_to_redshift)

The CMS Lite Metadata microservice emerged from a specialized use case of the S3 to Redshift microservice which required additional logic to build Lookup tables and Dictionary tables, as indicated though input data columns containing nested delimiters. To do so, it processes a single input `csv` file containing metadata about pages in CMS Lite, to generate several batch CSV files as a batch process. It then runs the `COPY` command on all of these files as a single Redshift transaction. As with the S3 to Redshift Microservice, The `json` configuration files specify the expected form of input data and output options.

#### [Google API Microservices](./google-api)

The Google API microservices are a collection of scripts to automate the loading of data collected through various Google APIs such as the [Google My Business API](https://developers.google.com/my-business/) for Location and Driving Direction insights; and the [Google Search Console API](https://developers.google.com/webmaster-tools/) for Search result analytics. Upon accessing the requested data, the Google API microservices build an output `csv` file containing that data, and stores it into S3. From there, the loading of data from S3 to Redshift follows very closely to the flow described in the S3 to Redshift microservice.

#### [Secure File Transfer System Microservice](./sfts)

The [`/sfts`](./sfts) folder contains the Secure File Transfer System microservice. This was configured first to support Performance Management and Reporting Program (PMRP) data exchange. This microservice is triggered to run after the successful transfer of PMRP date into Redshift. The microservice first generates an object in S3 from the output of a Redshift transaction modelling PMRP data with other GDX Analytics data, and then transfers that object from S3 to an upload location on BCGov's Secure File Transfer Service. The microservice is two scripts; one to generate the objets in S3 based on Redshift queries (`redshift_to_s3.py`) and one to transfer previously un-transferred files from S3 to SFTS (`s3_to_sfts.py`).

#### [Shared components](./lib)

The [`/lib`](./lib) folder contains the common components. As our microservices grow we are aiming to create shared patterns of use across them, and then modularize those shared patterns as reusable code. Eventually the components package may comprise a packaged application.

## Related Repositories

### [GDX-Analytics](https://github.com/bcgov/GDX-Analytics)

 * This is the central repository for work by the GDX Analytics Team.

## Getting Help or Reporting an Issue

For inquiries about starting a new analytics account please contact the GDX Analytics Team.

## How to Contribute

If you would like to contribute, please see our [CONTRIBUTING](CONTRIBUTING.md) guideleines.

Please note that this project is released with a [Contributor Code of Conduct](CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.

## License
```
Copyright 2015 Province of British Columbia

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
```
