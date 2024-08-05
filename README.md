# MongoDB Utilities for JVM_Mongo

This repository contains a collection of C# classes and interfaces designed to facilitate operations with MongoDB databases. The utilities include basic CRUD operations, aggregation pipelines, and support for bucket-style collections.

## Overview

The library is structured around several key components:

1. **AggregationSpecification**: Provides a way to define and execute MongoDB aggregation pipelines.
2. **BasicMongoGateway**: Offers a generic gateway for performing CRUD operations on MongoDB collections.
3. **ExternalBucketGateway**: Manages bucket-style collections in MongoDB, useful for handling large datasets.
4. **MongoDb**: Handles MongoDB client connections and configuration, including custom serializers for BSON.

## Classes and Interfaces

### AggregationSpecification

- **AggregationSpecification<T>**: An abstract record for defining aggregation specifications.
- **Group**: A record type for grouping documents in a pipeline.

### BasicMongoGateway

- **IBasicMongoGateway<T>**: Interface defining CRUD operations.
- **BasicMongoGateway<T>**: Implementation of `IBasicMongoGateway<T>`, with support for index creation.

### ExternalBucketGateway

- **IExternalBucketGateway**: Interface for managing bucket collections.
- **ExternalBucketGateway**: Implementation providing operations like lazy reading and bucket deletion.

### MongoDb

- **MongoDb**: Manages MongoDB client and database access, includes BSON configuration.
- **DictionarySerializer**: Custom dictionary serializer for BSON.
- **EnumStringSerializer**: Enum serializer that represents enums as strings.

## Usage

To use these utilities, include the necessary classes in your project and configure your MongoDB connection using the `MongoDb` class. Implement specific operations by utilizing the gateways provided.

Example of setting up a MongoDB connection:

```csharp
var mongoDb = new MongoDb("your_connection_string");
var database = mongoDb.GetDatabase("your_database_name");
