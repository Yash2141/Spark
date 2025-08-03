# Apache Spark

# what is Apache spark?

Apache spark is a unified computing engine and set of libraries for parallel data processing on computer cluster

**what is unified?**
spark is designed to support wide range of task over the same computing engine.
For ex.  data scientist, data analyst and data engineer all can use the same platform for their analysis, transformation or modelling.

So, instead of needing separate tools (or frameworks) for different tasks like batch processing, real-time streaming, machine learning, and SQL queries, Spark provides one big toolbox that can handle all those jobs. It's like having a Swiss Army knife for data processing. You can do lots of different things with it, all in one place, without needing to switch between different tools or learn new ones. That's the unified part of Spark!

**what is Computing engine**

spark is limited to a computing engine. It doesn’t store the data.
spark can connect with different data sources  like S3, HDFS, JDBC/ODBC, Azure storage etc
spark works with almost all data storage system.

The computing engine in Spark refers to the core processing capability that allows Spark to perform various data processing tasks efficiently and in parallel across multiple machines. Essentially, it's the powerhouse behind Spark's ability to handle large-scale data processing.

Spark's computing engine is responsible for tasks such as:
1. **Distributing data:** It splits up large datasets and distributes them across multiple computers in a cluster so they can be processed in parallel.
2. **Performing computations:** It executes the actual data processing tasks, whether it's filtering, aggregating, joining, or applying complex algorithms.
3. **Optimizing performance:** It includes mechanisms for optimizing the execution of tasks, such as minimizing data movement between nodes, caching intermediate results, and parallelizing operations to make the processing as fast and efficient as possible.
4. **Ensuring fault tolerance:** It incorporates mechanisms to handle failures gracefully, so if a node in the cluster fails during processing, Spark can recover and continue the computation without losing data or progress.
Overall, the computing engine is the heart of Apache Spark, responsible for making distributed data processing fast, efficient, and reliable.

**what is Computer cluster?**

a computer cluster in Spark is like a team of computers working together to solve big problems. Instead of just one computer doing all the work, a cluster involves multiple computers, each contributing their power to handle large amounts of data and complex tasks.

Think of it as a group project where each member has a specific role to play. In a Spark cluster, each computer, called a node, has its own processing power and memory. When you run a Spark program, it breaks down the tasks and data into smaller pieces and distributes them across these nodes. Then, each node works on its assigned part simultaneously.

## **Why Spark Came into the Picture**?

### **Before Spark:**

**1.Traditional Databases**: Systems like Oracle, MySQL, and SQL Server were designed to handle structured data, which means data that is organized in a specific format, like tables with rows and columns.

2. **Data Limitations**: These traditional databases could manage data up to a certain size and complexity, but they struggled with the increasing scale and diversity of data brought about by the internet and modern applications.

### **The Big Data Era:**

Data started coming from diverse sources like social media, sensors, logs, and more.

This data is not only vast in amount but also varied in format (structured, semi-structured, and unstructured).
With the growth of the internet and digital technologies, data started to explode in terms of the **three V's**:

**Volume**: The amount of data generated grew tremendously, making it difficult for traditional    databases to store and process it efficiently.

**Velocity**: The speed at which new data is generated and needs to be processed also increased. Traditional systems couldn't keep up with the real-time data processing requirements.

**Variety**: Data started coming in different formats, not just structured tables but also unstructured (like text, images, videos) and semi-structured (like JSON, XML) data.

**Challenges with Big Data:**

**1.Storage:** Traditional systems couldn’t handle the vast amounts of data efficiently.
2.**Processing:** Analyzing and processing this data required significant computational power (CPU and RAM).
**Approaches to Handle Big Data:**

1. **Monolithic Approach (Vertical Scaling):**
    - Adding more power (CPU, RAM) to a single machine.
    - **Issues:**
        - **Expensive:** High-end hardware costs are very high.
        - **Low Availability:** Single points of failure; if the machine goes down, the system becomes unavailable.
2. **Distributed Approach (Horizontal Scaling):**
    - Using a cluster of many machines (nodes) to share the load.
    - **Advantages:**
        - **Economical:** Uses commodity hardware, which is cheaper.
        - **High Availability:** If one node fails, others can take over, ensuring the system remains operational.

### **Enter Apache Spark:**

Apache Spark was developed to address these big data challenges. Here's how Spark addresses the issues:

1. **Distributed Computing**: Spark can distribute data processing tasks across a cluster of computers, which means it can handle much larger datasets than a single machine could.
2. **In-Memory Processing**: Spark performs operations in memory, which speeds up data processing significantly compared to disk-based processing systems like Hadoop MapReduce.
3. **Unified Framework**: Spark provides a single framework to handle various types of data processing tasks:
    - **Batch Processing**: Handling large volumes of data in bulk.
    - **Stream Processing**: Real-time data processing.
    - **Machine Learning**: Using its MLlib library for building and deploying machine learning models.
    - **Graph Processing**: With GraphX for graph computation.
    - **SQL Queries**: Through Spark SQL for structured data queries.

## **Hadoop vs. Spark: Detailed Comparison**

**1. Core Design and Processing Model**

**Hadoop:**

- **Processing Model:** Hadoop uses the MapReduce programming model for processing data. MapReduce splits tasks into a map phase (processing and sorting) and a reduce phase (aggregating results).
- **Data Handling:** Hadoop writes intermediate results to disk. After the map phase, data is written to the disk before the reduce phase reads it back. This I/O operation makes Hadoop slower.
- **Historical Context:** When Hadoop was built, the cost of RAM was high. Therefore, writing intermediate results to disk was a practical approach to avoid expensive in-memory operations.
- **Batch Processing:** Primarily designed for batch processing large volumes of data.

**Spark:**

- **Processing Model:** Spark uses Resilient Distributed Datasets (RDDs) and in-memory computing for processing data. It also uses DataFrames and Datasets for higher-level APIs.
- **Data Handling:** Spark processes data in memory whenever possible, which significantly speeds up computation by reducing I/O operations.
- **Batch and Streaming:** Designed for both batch processing and real-time stream processing.
- **Ease of Use:** Provides interactive shells (Scala, Python) and higher-level APIs, making it easier to write and debug code.

### **2. Performance and Speed**

**Hadoop:**

- **Disk I/O:** Slower due to heavy reliance on disk I/O. Intermediate data is stored on disk, making it less efficient for iterative processes.
- **Designed for Large Data:** Handles huge datasets well, but it might be slower because it processes data in batches.

**Spark:**

- **In-Memory Computing:** Faster because it works with data directly in memory. Great for repeated calculations and real-time data processing.
- **Optimized Performance:** Can be up to 100 times faster than Hadoop MapReduce for in-memory operations and up to 10 times faster for disk-based operations.

### **3. Ease of Development**

**Hadoop:**

- **Complexity:** Writing MapReduce jobs in Java can be complicated and lengthy.
- **Hive and Pig:** Tools like Hive (SQL-like queries) and Pig (data flow language) were developed to simplify the development process.
- **Development Tools:** Does not have interactive shells for easy development and debugging.

**Spark:**

- **Interactive Development:** Interactive shells (Scala, Python) allow for real-time testing and debugging.
- **High-Level APIs:** Supports high-level APIs (DataFrames, Datasets) and libraries for SQL (Spark SQL), machine learning (MLlib), graph processing (GraphX), and stream processing (Spark Streaming).
- **Ease of Use:** Easier to develop, test, and debug due to its unified and interactive nature.

### **4. Security**

**Hadoop:**

- **Security Features:** Includes Kerberos authentication, ACLs (Access Control Lists), and encryption options for data security.
- **Mature Security:** Has more mature and robust security features suitable for enterprise-grade applications.

**Spark:**

- **Security Limitations:** Initially had fewer built-in security features, but recent versions have improved with encryption, authentication through Hadoop's security model, and integration with Kerberos.
- **Enterprise Security:** Still considered less mature than Hadoop in terms of built-in security features.

### **5. Fault Tolerance and Data Handling**

**Hadoop:**

- **Data Replication:** Uses HDFS (Hadoop Distributed File System) which divides data into blocks and replicates them across the cluster to handle failures.
- **Reliability:** Designed for high fault tolerance with automatic data replication.

**Spark:**

- **Fault Tolerance:** Uses Directed Acyclic Graphs (DAGs) to track data lineage and recover lost data partitions efficiently.
- **In-Memory Data Handling:** Provides resilience by rebuilding RDDs from lineage information.
**Example:** Consider a DAG with processes: process1 → process2 → process3 → process4. If process3 fails, Spark knows how it was created from process2 and can recover it without redoing the entire workflow.

### **Example Use Cases**

**Hadoop:**

- **Log Processing:** Batch processing of log files for analysis.
- **Data Warehousing:** Storing and processing large datasets with tools like Hive.
- **Archive Storage:** Long-term storage and batch processing of massive datasets.

**Spark:**

- **Real-Time Analytics:** Processing streaming data from sources like Kafka in real-time.
- **Machine Learning:** Training and deploying machine learning models on large datasets using MLlib.
- **Interactive Data Analysis:** Quick data exploration and interactive queries with Spark SQL and DataFrames.

## how Apache Spark works, including its architecture

**Step-by-Step Process**

1. **Input Data (Disk)**: Data is read from a distributed storage system (e.g., HDFS, S3, local file system).
2. **Spark Context Creation**: A SparkContext object is created to coordinate the Spark application. This is the main entry point for Spark functionality.                                       spark context sets up internal services and establishes a connection to a spark execution environment.
3. **Data Loading**: Data is loaded into Resilient Distributed Datasets (RDDs) or DataFrames/Datasets.
4. **Transformations**: Transformations like **`map`**, **`filter`**, and **`flatMap`** are applied to the RDDs/DataFrames to create new RDDs/DataFrames. These operations are lazy and only define a lineage of operations.
5. **Actions**: Actions like **`collect`**, **`count`**, and **`saveAsTextFile`** trigger the execution of the transformations. This starts the actual data processing.
6. **Job Submission**: The SparkContext breaks the job into tasks and schedules them on the cluster.
7. **Task Execution**: Tasks are executed by Executors running on worker nodes. Executors process data in memory (RAM) for fast computation.
8. **Shuffling**: If necessary, data is shuffled (redistributed) across the cluster to perform operations like **`groupByKey`** or **`reduceByKey`**.
9. **Result Storage**: The final results can be collected back to the driver program or written to an output sink (disk, HDFS, S3, etc.).

![Screenshot from 2024-05-19 18-31-57.png](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Screenshot_from_2024-05-19_18-31-57.png)

### **Explanation of the Diagram**

- **Input Data (Disk)**: Data is initially stored in a distributed file system.
- **Spark Context**: Coordinates the Spark application, managing resources and scheduling tasks.
- **Transformations (Lazy)**: Operations that define a sequence of data manipulations but do not execute until an action is called.
- **Actions (Trigger Execution)**: Operations that trigger the execution of transformations and produce output.
- **Memory (RAM)**: Executors process data in memory, allowing for fast computation.
- **Executors**: Each executor runs on a worker node and executes tasks. They read data, perform transformations, and store intermediate data in memory.
- **Shuffling**: Data is moved between executors if necessary for operations like grouping or joining.
- **Output Data (Disk)**: Final processed data is written back to a distributed file system or other storage.

### **Execution Flow**

1. **Input Data**: Data is read from disk.
2. **Spark Context**: Initializes and manages the application.
3. **RDD/DataFrame Creation**: Data is loaded into RDDs/DataFrames.
4. **Transformations**: Applied to the RDDs/DataFrames to define the computation.
5. **Actions**: Trigger the execution of the computation.
6. **Task Scheduling**: The job is divided into tasks and distributed across the cluster.
7. **Task Execution**: Executors perform the tasks in memory.
8. **Shuffling**: Data is shuffled if necessary.
9. **Output Data**: Results are written back to disk or collected by the driver.

## how Hadoop processes data, including its architecture

**Step-by-Step Process**

1. **Input Data (Disk)**: Data is stored in the Hadoop Distributed File System (HDFS).
2. **Job Submission**: A MapReduce job is submitted to the Hadoop cluster.
3. **Input Splits**: The input data is split into smaller chunks called input splits.
4. **Map Phase**: Each input split is processed by a Mapper, which produces intermediate data key-value pairs.
5. **Shuffle and Sort**: The intermediate data key-value pairs are shuffled and sorted by key.
6. **Reduce Phase**: The sorted data is processed by Reducers, which aggregate the data and produce the final output.
7. **Output Data (Disk)**: The final output is written back to HDFS.

![Screenshot from 2024-05-19 18-29-08.png](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Screenshot_from_2024-05-19_18-29-08.png)

## Spark Eco-System

![Screenshot from 2024-05-23 15-49-15.png](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Screenshot_from_2024-05-23_15-49-15.png)

- **Spark Ecosystem**: includes all the different tools and libraries built around Apache Spark for various data processing tasks.

**DataFrame API**: A high-level API for working with structured data. It provides a programming abstraction similar to dataframes in R and Python (Pandas).

**Spark SQL**: A module for working with structured data using SQL queries. It allows users to run SQL queries on Spark data, enabling seamless integration with existing SQL-based tools and workflows.

**Streaming**: High-level APIs for processing real-time streaming data. It provides support for ingesting, processing, and analyzing data streams in real-time.

**Spark Core**: The foundational component of the Spark ecosystem, providing distributed task scheduling, memory management, fault recovery, etc.

**Python/Java/R/Scala**: Different programming languages supported by Spark for developing applications. Developers can choose the language they are most comfortable with for writing Spark applications.

**Spark Engine**: The underlying engine that powers all the components of the Spark ecosystem. It includes components like the DAG scheduler, task scheduler, memory management, and execution engine.

In simple terms, the Spark ecosystem provides various tools and libraries for different data processing needs, whether it's analyzing structured data, running SQL queries, processing real-time data streams, or developing applications in different programming languages. At its core, Spark provides the engine that orchestrates and executes these tasks efficiently across distributed clusters.

The Spark Ecosystem includes all the different tools and libraries that work alongside Apache Spark to help with different tasks related to processing data.

The Spark Ecosystem includes all the different tools and libraries that work alongside Apache Spark to help with different tasks related to processing data.

## Spark Core vs Spark Engine

**Spark Core: 
Definition**:  Spark Core is the foundational component of Apache Spark. It provides the basic functionalities required for distributed data processing, including task scheduling and monitoring tasks across the cluster, memory management, fault recovery, and interaction with storage systems.

**Functionality**: It includes task scheduling, memory management, fault tolerance, and interaction with storage systems.

**Example**: Spark Core manages the distribution of tasks across a cluster, ensures fault tolerance by tracking lineage of operations, and optimizes data processing with in-memory computing.

**Easy-way:**  Imagine you're baking a cake. Spark Core is like the kitchen where you mix all the ingredients, preheat the oven, and set the timer. It's the foundation that ensures everything runs smoothly.

**Spark Engine:
Definition**: The Spark Engine is the powerhouse that takes your code and makes it run fast and efficiently across multiple computers.

**Functionality**: It provides a comprehensive environment for executing Spark applications, processing data, and implementing various functionalities like SQL queries, streaming, machine learning, and graph processing.

**Example**: The Spark Engine powers the execution of Spark applications, orchestrating tasks across clusters using Spark Core, processing data with Spark SQL, analyzing streams with Spark Streaming, implementing machine learning models with MLlib, and performing graph processing with GraphX. the Spark Engine executes these operations efficiently across the cluster using Spark Core and other relevant components

**Easy-way:** Think of the Spark Engine as a team of chefs working together to cook a big meal. Each chef handles a different task, like chopping vegetables or stirring the sauce. They communicate and coordinate with each other to get the job done quickly and efficiently. Similarly, the Spark Engine breaks down your tasks, distributes them to different computers (called nodes), and brings all the results together, just like the chefs working together to prepare a delicious meal.

The Spark Engine talks to the cluster manager and requests resources for running a job. For example, it might say, "I need 4 executors, each with 20GB of memory, and one driver with 20GB of memory, totaling 100GB." The cluster manager then checks if there are enough resources available in the cluster. If there are, it allows the job to run. If not, it puts the job in a queue to wait until enough resources become available.
This process ensures that Spark jobs get the necessary resources to run efficiently without overloading the cluster.

## **Transformation and Action / Dag and Lazy evaluation/Linage Graph**

**Transformation:** Transformations are operations that create a new dataset by applying some function to the existing dataset. These operations are lazy, meaning they do not compute the result immediately. Instead, they build up a logical execution plan (a Directed Acyclic Graph or DAG) that Spark will execute when an action is called.

**Type of transformation** 
their are two type of transformation
1. **Narrow Transformation:-** transformation that does’nt requiered data movement between the partition is called Narrow transformation.
 Examples: **`map`**, **`filter`**, **`flatMap`

2.Wide Transformation:-** transformation that requiered data movement between the partition is called wide transformation.
 Examples: **`groupBy`**, **`join`**, **`reduceByKey`

Action:-** Action executes all the related transformations to get the required data. Functions such as collect(), show(), count(), first().

**Lazy Evaluation:** Lazy Evaluation in Sparks means Spark will not start the execution of the process until an Action is called. Once an Action is called, Spark starts looking at all the transformations and creates a DAG. DAG is sequence of operations that need to be performed in a process to get the resultant output. If Spark could wait until an Action is called, it may merge some transformation or skip some unnecessary transformation and prepare a perfect optimized execution plan.

Lets take an example,
We have an employee data set. First we will perform a filter() operation (Narrow Transformation) then orderBy() and groupBy() operations (Wide Transformations).

```sql
df=spark.read.parquet("employee")

#Transformations
df_sal = df.filter("Salary > 6000")                       #1
df_exp = df_sal.filter("Experience_Years > 4")            #2
df_age = df_exp.filter("Age < 55")                        #3
df_gen = df_age.filter("Gender=='Male'")                  #4
df_mumbai=df.filter("Location=='Mumbai'")                 #5

df_ordered = df.orderBy("Salary")                         #6
df_sal2 = df_ordered.filter("Salary > 6000")              #7
df_exp2 = df_sal2.filter("Experience_Years > 3")          #8
df_age2 = df_exp2.filter("Age > 30")                      #9
df_gen2 = df_age2.filter("Gender=='Female'")              #10
df_pune_ord=df_gen2.filter("Location=='Pune'")            #11
df_pune_grp = df_pune_ord.groupBy("Gender").sum("Salary") #12

#Action
df_mumbai.show()   #1
df_pune_grp.show() #2
```

𝐖𝐡𝐚𝐭 𝐢𝐬 𝐋𝐢𝐧𝐞𝐚𝐠𝐞 𝐆𝐫𝐚𝐩𝐡 𝐢𝐧 𝐒𝐩𝐚𝐫𝐤?

⭕ The Lineage Graph is a directed acyclic graph (DAG) in Spark or PySpark that represents the dependencies between RDDs (Resilient Distributed Datasets) or DataFrames in a Spark application.

⭕ Every transformation in Spark creates a new RDD or DataFrame that is dependent on its parent RDDs or DataFrames.

⭕ The Lineage Graph Tracks all the operations performed on the input data, including transformations and actions, and stores the metadata of the data transformation steps.

⭕ It is a crucial component of Spark’s fault tolerance mechanism. Since RDDs are immutable, the Lineage Graph helps in reconstructing lost RDDs by recomputing their parent RDDs based on their transformations.

⭕ This feature enables Spark to recover lost data in case of node failures or other issues in the cluster.This also helps in optimizing the execution plan of Spark applications.

⭕ Spark uses the information in the Lineage Graph to optimize the DAG and perform transformations in a way that reduces data shuffling and increases parallelism. This optimization leads to faster execution times and efficient utilization of cluster resources.

## Spark Sql Engine

Q1. what is catalyst optimizer/ spark sql engine

Q2. why do you get analysis exception error

Q3. what is physical plane /spark plane

Q4. Is spark sql Engine is a compiler

Q5. how many phases are involve in spark

![Screenshot from 2024-05-26 17-30-07.png](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Screenshot_from_2024-05-26_17-30-07.png)

1. **SQL / DataFrame / Dataset:**
    - **SQL:** You can write SQL queries to interact with data.
    - **DataFrame:** A distributed collection of data organized into named columns, similar to a table in a relational database.
    - 
    - **Dataset:** A strongly-typed collection of objects mapped to a schema.
2. **Catalyst Optimizer / Spark SQL Engine:**
    - This is where Spark optimizes the logical plan of your queries or transformations. The Catalyst Optimizer analyzes and optimizes the execution plan.
3. **RDD (Resilient Distributed Dataset):**
    - After optimization, the logical plan is converted into a physical plan, which is represented as RDDs. RDDs are the low-level data structure that Spark uses for distributed processing.
    
    **Four Phases of spark sql engine**
    1. Analysis
    
    1. logical plan
    2. physical plan
    3. code generation
    
    ![Screenshot from 2024-05-26 18-03-23.png](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Screenshot_from_2024-05-26_18-03-23.png)
    
    1. **spark.read.csv:**
        - This is the initial step where Spark reads a CSV file into a DataFrame.
        - **catelog:-**  contain file table database etc
        - **Analysis:-**If Spark cannot find a required column during the resolution phase, it throws an **`AnalysisException`** error.
    2. **Unresolved Logical Plan:**
        - The initial logical plan is created but contains unresolved references, meaning that Spark hasn't yet verified the existence and types of the columns or tables.
    3. **Resolved Logical Plan:**
        - Spark resolves the references by verifying the schema and types, turning it into a resolved logical plan.
    4. **Optimized Logical Plan:**
        - The Catalyst Optimizer takes the resolved logical plan and applies various optimization rules to produce an optimized logical plan.
    5. **Physical Plan:**
        - The optimized logical plan is then converted into a physical plan, which consists of RDDs and the actual execution steps.
        
        ![Screenshot from 2024-05-26 17-38-34.png](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Screenshot_from_2024-05-26_17-38-34.png)
        

## What is RDD **and When Do We Need RDDs in Spark?**

An RDD (Resilient Distributed Dataset) in Spark is a collection of data that is distributed across multiple computers in a cluster. It allows you to perform parallel and fault-tolerant computations on large datasets.

An RDD is like a big list of data that is split into chunks and spread out over many computers. It lets you do operations on this data in a fast and reliable way, even if some computers fail.

### **Key Points:**

- **Distributed:** Data is spread across many computers.
- **Resilient:** Can recover from failures.
- **Parallel:** Can process data quickly by working on multiple pieces at the same time.

### **Example:**

Imagine you have a huge list of numbers and you want to find the square of each number. Using an RDD, you can split this list into smaller parts, spread them across different computers, and calculate the squares in parallel. If one computer fails, Spark can automatically use the remaining computers to finish the task.

In code, it might look like this:

```scala
val numbers = sc.parallelize(Seq(1, 2, 3, 4, 5)) // Create an RDD from a list of numbers
val squares = numbers.map(n => n * n)           // Transformation: calculate the square of each number
val result = squares.collect()                  // Action: collect the results back to the main program
result.foreach(println)                         // Print the results

```

In this example, the numbers are distributed across the cluster, processed in parallel, and the results are resilient to failures.

### **Key Characteristics of RDDs**

1. **Resilience:** RDDs can automatically recover from node failures. They achieve this through lineage information, which tracks the series of transformations that created the dataset. If a partition of an RDD is lost, Spark can recompute it using the original transformations.
2. **Distributed:** Data in an RDD is distributed across multiple nodes in a cluster. This distribution allows Spark to process large datasets efficiently by parallelizing operations.
3. **Immutable:** Once created, RDDs cannot be modified. This immutability helps ensure consistency and fault tolerance, as the original RDD can always be reconstructed from its lineage.
4. **Lazy Evaluation:** Transformations on RDDs are not executed immediately. Instead, they build up a logical execution plan that Spark optimizes and executes only when an action is called.

### **Types of Operations**

RDDs support two types of operations: transformations and actions.

- **Transformations:** These are operations that create a new RDD from an existing one. They are lazy, meaning they build up the execution plan but do not immediately execute.
    - Examples: **`map`**, **`filter`**, **`flatMap`**, **`reduceByKey`**
- **Actions:** These operations trigger the execution of the transformations. They return a result to the driver program or write data to external storage.
    - Examples: **`collect`**, **`count`**, **`saveAsTextFile`**, **`reduce`**

### **Example**

Here’s a simple example of creating and using an RDD:

```scala
scalaCopy code
// Initialize SparkContext
val sc = new SparkContext(new SparkConf().setAppName("Example").setMaster("local"))

// Create an RDD from a collection
val numbers = sc.parallelize(Seq(1, 2, 3, 4, 5))

// Transformation: Multiply each number by 2
val doubled = numbers.map(n => n * 2)

// Transformation: Filter numbers less than or equal to 6
val filtered = doubled.filter(n => n <= 6)

// Action: Collect the results and print them
val result = filtered.collect()
result.foreach(println)

// Output: 2, 4, 6

```

### **Benefits of RDDs**

- **Fault Tolerance:** Through lineage information, RDDs can be recomputed in case of failures.
- **Parallelism:** Operations on RDDs can be parallelized across multiple nodes, making it suitable for large-scale data processing.
- **Flexibility:** RDDs can handle both structured and unstructured data, offering a wide range of transformations and actions.
- **Immutability:** Ensures consistency and simplifies reasoning about data transformations.

### **When Do We Need RDDs in Spark?**

In simple terms, we use RDDs in Spark when we need to:

1. **Handle Unstructured Data:** When dealing with raw, unstructured data that doesn’t fit into tables or databases.
2. **Apply Custom Transformations:** When we need to perform complex transformations or operations that are not easily supported by higher-level APIs like DataFrames or Datasets.
3. **Ensure Fault Tolerance:** When we need robust fault tolerance with automatic recovery from failures.
4. **Control Over Data Partitioning:** When we need fine-grained control over how data is distributed and partitioned across the cluster.

### **Simple Example**

Let’s say we have a text file with raw log data, and we want to filter out lines containing the word "error" and then count those lines.

**Using RDDs:**

1. **Read Raw Data:**
    
    ```scala
    val sc = new SparkContext(new SparkConf().setAppName("RDDExample").setMaster("local"))
    val rawLogs = sc.textFile("path/to/logfile.txt")
    ```
    
2. **Transformation:** Filter lines containing "error".
    
    ```scala
    val errorLines = rawLogs.filter(line => line.contains("error"))
    ```
    
3. **Action:** Count the number of error lines.
    
    ```scala
    val errorCount = errorLines.count()
    println(s"Number of error lines: $errorCount")
    ```
    

**Step-by-Step Explanation:**

1. **Read Raw Data:** We create an RDD from a text file containing raw log data.
    
    ```scala
    val rawLogs = sc.textFile("path/to/logfile.txt")
    ```
    
2. **Transformation:** We apply a filter transformation to create a new RDD (**`errorLines`**) that only contains lines with the word "error". This transformation is lazy and doesn’t run immediately.
    
    ```scala
    val errorLines = rawLogs.filter(line => line.contains("error"))
    ```
    
3. **Action:** We call the **`count`** action to trigger the computation and count the number of lines in **`errorLines`**. This action executes all the transformations and returns the result.
    
    ```scala
    val errorCount = errorLines.count()
    println(s"Number of error lines: $errorCount")
    ```
    

**Why Use RDDs Here?**

- **Raw, Unstructured Data:** The log file is raw and unstructured.
- **Custom Transformation:** Filtering specific lines requires a custom transformation.
- **Fault Tolerance:** RDDs provide automatic recovery from failures.
- **Fine-Grained Control:** We may want control over how the data is processed and distributed.

### **When to Use RDDs vs. DataFrames/Datasets**

- **Use RDDs:**
    - When working with raw, unstructured data.
    - When performing custom transformations or low-level operations.
    - When needing fine-grained control over data processing.
- **Use DataFrames/Datasets:**
    - When working with structured data and schemas.
    - When performing SQL-like queries and operations.
    - When benefiting from optimizations provided by the Catalyst optimizer.
    
    **Why we should not used rdd?**
    
    you might want to avoid using RDDs in Spark when:
    
    1. **Structured Data:** If your data has a well-defined schema, like tables in a database, DataFrames or Datasets are usually a better choice. They offer easier-to-use APIs and optimizations that RDDs don't have.
    2. **SQL-Like Operations:** If you're comfortable with SQL or need to perform SQL-like operations, DataFrames or Datasets provide a more intuitive interface with built-in optimizations.
    3. **Complexity:** RDDs require more manual management and are lower-level compared to DataFrames or Datasets. If you can achieve your goals with higher-level abstractions, it's often simpler and more efficient to do so.
    4. **Performance:** While RDDs offer flexibility and fine-grained control, they may not always be as optimized for performance as DataFrames or Datasets, especially for common operations like filtering, joining, and aggregating.
    
    ### **Example:**
    
    Let's say you have a CSV file with structured data containing information about users, and you want to filter out users from a specific country.
    
    **Using RDDs:**
    
    ```scala
    val rawData = sc.textFile("users.csv")
    val users = rawData.map(line => line.split(","))
    val filteredUsers = users.filter(fields => fields(2) == "USA")
    ```
    
    **Using DataFrames:**
    
    ```scala
    val df = spark.read.option("header", "true").csv("users.csv")
    val filteredDF = df.filter("country == 'USA'")
    ```
    

## Spark job, stages,  task

****Q1.what is a job, stage and task in spark?
Q2.how many jobs will be created in the given question?
Q3.how many stages will be created?
Q4.how many tasks will be created?

![Screenshot from 2024-05-29 16-17-16.png](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Screenshot_from_2024-05-29_16-17-16.png)

**1.Job:** A job in Spark refers to a sequence of transformations on data. Whenever an action like count(), first(), collect(), and save() is called on RDD (Resilient Distributed Datasets), a job is created. A job could be thought of as the total work that your Spark application needs to perform, broken down into a series of steps.

Consider a scenario where you’re executing a Spark program, and you call the action `count()` to get the number of elements. This will create a Spark job. If further in your program, you call `collect()`, another job will be created. So, a Spark application could have multiple jobs, depending upon the number of actions.

2.**Stages:** A stage is a logical unit of work within a Spark job. It represents a set of tasks that can be executed together, typically resulting from a narrow transformation (e.g., `map` or `filter`) or a shuffle operation (e.g., `groupByKey` or `reduceByKey`).

A job in Spark is divided into stages. A stage is a collection of tasks that can be executed together, without any data shuffling between them. Stages are determined by the RDD lineage and the transformations applied to it. Spark tries to minimize the number of stages required to execute a job by grouping together transformations that can be computed in parallel without data movement between them.

**Example**: Suppose you have a Spark job that reads data from a file, filters it, and then performs a group-by operation. This job will typically have two stages: one for the filter operation and one for the group-by operation.

3.**Tasks**: Tasks are the smallest units of work in Spark. Each task corresponds to the execution of a single partition of an RDD on a single executor. Spark automatically breaks down each stage into tasks and schedules them for execution on the available executors.

**Example**: If you have a RDD with 100 partitions and you submit a job that involves a map operation on this RDD, Spark will create 100 tasks, each processing one partition of the RDD.

![Screenshot from 2024-05-29 17-11-38.png](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Screenshot_from_2024-05-29_17-11-38.png)

Image in phone putted here

## what is repartition and coalesce / repartition vs partition

Q1.what is repartitioning in spark?
Q2.what is coalesce in spark?
Q3.which one will you choose and why?
Q4.repartioning vs coalesce?

### **Repartition**

- **Purpose:** **`repartition`** increases or decreases the number of partitions in the dataset.
- **Use Case:** Used when you need to increase the number of partitions for parallelism or decrease it for reducing overhead.
- **Implementation:** It performs a full shuffle of the data across the nodes in the cluster, which can be expensive in terms of performance.
- good with working on .equal size of partitions because spark is design to work with equal size of partitions.
- **Example Use Case:**
    - If you start with a dataset that has only 2 partitions but you have a cluster with 8 cores, using **`repartition`** to increase the number of partitions to 8 will help utilize all cores, speeding up processing.
        
        ```scala
        
        val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6), 2)
        val repartitionedRDD = rdd.repartition(8)
        
        ```
        

repartitions will shuffle data in all the partitions,therefore the networks usage between the executors will be high and it will impacts the performance. 

repartions disadvantage is full shuffle if there is a lot of shuffle the processing will be slow 

### **Coalesce**

- **Purpose:** **`coalesce`** reduces the number of partitions in the dataset.
- **Use Case:** Used to reduce the number of partitions to optimize performance, especially when a dataset has too many small partitions.
- **Implementation:** It tries to avoid a full shuffle and moves data from multiple partitions into fewer partitions. It is more efficient than **`repartition`** when decreasing the number of partitions.
- coalesce uses existing partitions to minimize the amount of data that’s shuffled
- **Example Use Case:**
    - If a dataset is divided into 100 partitions but the data volume is small, using **`coalesce`** to reduce the partitions to a more reasonable number (e.g., 10) can improve performance by reducing management overhead.

```scala
val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6), 100)
val coalescedRDD = rdd.coalesce(10)
```

for ex if you have 3 partitions and you want to reduce it to 2 coalesce will move the 3rd partition data to partitions 1 and 2. partition 1 and 2 will remains in the same container.

coalesce disadvantage it will create  unequal size of partitions

### **Key Differences:**

1. **Shuffling:**
    - **`repartition`** performs a full shuffle of the data.
    - **`coalesce`** tries to minimize shuffling by combining partitions without a full shuffle.
2. **Performance:**
    - **`repartition`** is more expensive due to the full shuffle.
    - **`coalesce`** is more efficient for reducing partitions because it avoids a full shuffle.
3. **Use Case:**
    - Use **`repartition`** when increasing partitions or when reducing partitions requires a significant data shuffle for even distribution.
    - Use **`coalesce`** for efficiently reducing the number of partitions without requiring a full shuffle.
    
    ### **Why Do We Use `repartition` and `coalesce` in Spark?**
    
    ### **The Problem Before Using `repartition` and `coalesce`:**
    
    1. **Imbalanced Data Processing:**
        - **Too Few Partitions:** If an RDD or DataFrame has too few partitions, not all available CPUs or cores in the cluster will be utilized. This can cause some tasks to take longer to complete while others sit idle, leading to inefficient resource use and slower performance.
        - **Too Many Partitions:** If there are too many partitions, each partition might be very small. This can lead to high overhead from managing too many small tasks, causing inefficient execution and increased job scheduling time.
    2. **Data Skew:**
        - When data is not evenly distributed across partitions, some partitions may contain significantly more data than others. This causes some tasks to take much longer to complete, resulting in overall job delay.

**Partition

What is it?**
A partition is a small chunk of data that makes up a larger dataset.

**Example:**
Imagine you have a big pile of books. You decide to divide the books into smaller stacks, with each stack representing a partition. Each partition contains a subset of the books.

**Why is it Important?**
Partitions allow Spark to split up the workload and distribute it across multiple machines in a cluster. This parallel processing helps in performing computations faster.

**Repartition:

What is it?**
Repartitioning is the process of changing the number of partitions in a dataset.

**Example:**
Suppose you have divided your books into 10 stacks (partitions), but you realize that some stacks have too many books while others have too few. You decide to redistribute the books by either merging some stacks together or splitting some stacks apart to balance the workload. This process is repartitioning.

**Why is it Important?**
Repartitioning helps in optimizing performance by balancing the workload across partitions. It ensures that each partition has a similar amount of data, maximizing the utilization of resources and improving parallelism.

## What are the join strategies in spark?

1. **Shuffle Hash Join**
2. **Broadcast  Join**
3. **Sort Merge Join**

**1.Shuffle Hash Join:** is a join strategy used in Spark when both datasets are large and cannot fit into memory. This method involves shuffling data across the network so that records with the same key end up on the same node, allowing them to be joined together using a hash table.

**How Shuffle Hash Join Works:

1.Partitioning:** Spark repartitions the two datasets based on the join key, ensuring that records with the same key are sent to the same partition across the cluster.

2.**Shuffling:** Data is shuffled across the nodes to align the join keys.

3.**Hash Join:** Within each partition, Spark builds a hash table for the smaller of the two datasets and probes it with the larger dataset.

**Create Sample Datasets:**

```scala
val users = sc.parallelize(Seq(
  (1, "Alice"),
  (2, "Bob"),
  (3, "Charlie")
))

val transactions = sc.parallelize(Seq(
  (1, "Order1"),
  (2, "Order2"),
  (3, "Order3"),
  (1, "Order4"),
  (2, "Order5")
))
```

**Perform the Shuffle Hash Join:**

```scala
val joined = users.join(transactions)
joined.collect().foreach(println)
```

1. **Partitioning and Shuffling:** Spark will repartition both **`users`** and **`transactions`** RDDs by **`user_id`**. This ensures that all records with the same **`user_id`** end up in the same partition across both datasets.
2. **Building Hash Table:** For each partition, Spark builds a hash table using the **`users`** dataset (assuming it fits into memory within the partition).
3. **Probing Hash Table:** Spark then probes the hash table with the **`transactions`** dataset to find matching **`user_id`** entries and performs the join.

![Screenshot from 2024-05-31 14-56-48.png](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Screenshot_from_2024-05-31_14-56-48.png)

### **When to Use Shuffle Hash Join:**

1. **Both Datasets are Large:**
Shuffle Hash Join works best when both datasets are large and cannot fit into memory.
2. **Similar Data Distribution:**
If both datasets have a similar distribution of keys,It ensures that records with the same key end up on the same node, minimizing data movement during the join process.
3. **Balanced Data Skew:**
If there is minimal data skew (i.e., each key has a similar number of associated records), Shuffle Hash Join can perform well.
4. **Optimized for Parallelism:**
It utilizes multiple nodes in the cluster to perform the join operation concurrently, leading to improved performance.

**When to Avoid Shuffle Hash Join:**

1. **Small Datasets:**
2. **Limited Resources:**
When the cluster resources (e.g., memory, CPU, and network bandwidth) are limited. 
3. **Complex Join Conditions:**
If the join conditions are complex and involve multiple keys or non-equijoins, Shuffle Hash Join might not be the most suitable option.
4. **Data Skew:**
Shuffle Hash Join may lead to uneven processing and performance issues. 

### **2.Sort Merge Join in Spark**

**Sort Merge Join** is a join strategy used in Spark to join two datasets that are already sorted based on the join key. It involves merging the sorted datasets together, similar to how merging two sorted lists works.

### **How Sort Merge Join Works:**

1. **Sorting:** Both datasets are sorted based on the join key. This can be achieved through partitioning and sorting before the join operation.
2. **Merging:** The sorted datasets are merged together based on the join key. This is done efficiently because both datasets are already sorted.

**Example:**

Let's say you have two sorted datasets:

- **Employees:** Contains employee information sorted by **`employee_id`**.
- **Salaries:** Contains salary information sorted by **`employee_id`**.

You want to join these datasets based on **`employee_id`**.

### **Step-by-Step Example:**

1. **Create Sample Sorted Datasets:**

```scala
val employees = sc.parallelize(Seq(
  (1, "Alice"),
  (2, "Bob"),
  (3, "Charlie")
)).sortBy(_._1)

val salaries = sc.parallelize(Seq(
  (1, 50000),
  (2, 60000),
  (3, 70000)
)).sortBy(_._1)
```

1. **Perform the Sort Merge Join:**

```scala
val joined = employees.join(salaries)
joined.collect().foreach(println)
```

### **Explanation:**

1. **Sorting:** Both **`employees`** and **`salaries`** datasets are sorted based on the **`employee_id`**.
2. **Merging:** Spark efficiently merges the sorted datasets together, joining records with the same **`employee_id`**.

### **Output:**

```scala
(1, ("Alice", 50000))
(2, ("Bob", 60000))
(3, ("Charlie", 70000))
```

### **When to Use Sort Merge Join:**

1. **Already Sorted Datasets:**
Sort Merge Join is most effective when both datasets are already sorted based on the join key (**`employee_id`** in the example).
2. **Large Datasets:**
Sorting large datasets can be more efficient than shuffling and hashing for join processing, making Sort Merge Join a preferred strategy.
3. **Ordered Processing Required:**
When the order of the output records matters, Sort Merge Join ensures that the joined result maintains the sorted order of the input datasets.
4. **Balanced Resource Utilization:**
Sort Merge Join can help in balancing resource utilization across the cluster by avoiding excessive shuffling and network communication.

**When to Avoid Sort Merge Join:
Unsorted Datasets:**
If the datasets are not sorted based on the join key, using Sort Merge Join can be inefficient.
Significant data skew exists, leading to uneven processing.
Memory resources are limited, and sorting large datasets may lead to performance issues.
Join conditions are complex or involve non-equijoins.

## **3.Broadcast Join in Spark**

**Broadcast Join** is a join strategy in Spark where one dataset (usually smaller) is broadcasted to all the worker nodes in the cluster, allowing each node to perform a local join with the larger dataset. This strategy is particularly useful when one dataset is small enough to fit into memory across all nodes, reducing the need for shuffling data across the network.

**How Broadcast Join Works:
Broadcasting:**
The smaller dataset (broadcast dataset) is replicated and sent to every worker node in the cluster. This is typically done using Spark's broadcast mechanism.

**Local Join:**
Each worker node performs a local join between the broadcast dataset and its partition of the larger dataset.
**Example Using Spark SQL:**
Suppose we have two datasets:

1. **`employees`**: Contains information about employees including their IDs, names, and country codes.
2. **`countries`**: A lookup table containing country codes and their corresponding names.

We want to join these datasets based on the country code to get the employees' country names.

### **Create Sample Datasets:**

```scala
 
// Sample data for employees
val employeesData = Seq(
  (1, "Alice", "US"),
  (2, "Bob", "UK"),
  (3, "Charlie", "CA")
)
val employeesDF = spark.createDataFrame(employeesData).toDF("employee_id", "name", "country_code")

// Sample data for countries (lookup table)
val countriesData = Seq(
  ("US", "United States"),
  ("UK", "United Kingdom"),
  ("CA", "Canada")
)
val countriesDF = spark.createDataFrame(countriesData).toDF("country_code", "country_name")

```

1. **Broadcast the Lookup Table:**

```scala

// Broadcast the countries DataFrame
val broadcastCountries = spark.sparkContext.broadcast(countriesDF)

```

1. **Perform the Broadcast Join Using Spark SQL:**

```scala

// Register the DataFrames as temporary views
employeesDF.createOrReplaceTempView("employees")
broadcastCountries.value.createOrReplaceTempView("countries")

// Perform the broadcast join using Spark SQL
val joinedDF = spark.sql(
  """
  SELECT e.employee_id, e.name, c.country_name
  FROM employees e
  JOIN countries c ON e.country_code = c.country_code
  """
)

```

1. **Show the Result:**

```scala
// Show the joined DataFrame
joinedDF.show()

```

     **Explanation:**

- We create DataFrames for both **`employees`** and **`countries`** datasets.
- The **`countries`** DataFrame is broadcasted using **`spark.sparkContext.broadcast()`** to make it available on all nodes.
- Both DataFrames are registered as temporary views (**`employees`** and **`countries`**) to be used in Spark SQL queries.
- We perform a SQL join between the **`employees`** and **`countries`** tables based on the **`country_code`** column.
- The result is a DataFrame (**`joinedDF`**) containing the employee ID, name, and country name.

## **Spark Out of Memory Issue**

refrence: [https://medium.com/swlh/spark-oom-error-closeup-462c7a01709d](https://medium.com/swlh/spark-oom-error-closeup-462c7a01709d)

OOM stands for "Out Of Memory." In the context of Apache Spark, an OOM error occurs when Spark applications exceed the memory limits allocated for their execution, causing the program to crash or fail. This issue can happen at various stages of Spark processing, such as during data shuffling, caching, or any operation that requires substantial memory.

**Driver Memory Issues
1. Collect Operation in Driver:**
The `collect` operation in Spark is a common source of out-of-memory issues in the driver. The `collect` operation is used to retrieve data from distributed Spark workers and consolidate it on the driver. When the collected data is too large to fit into the driver’s memory, it can lead to an out-of-memory error. Below is an example of how you can run into this issue in PySpark:

```sql
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName(“CollectExample”).getOrCreate()
df = spark.read.parquet(“large_data.parquet”)

collected_data = df.collect() # Collecting data to the driver
```

**2.Broadcast Join in Spark
Broadcast joins** are used in Spark to optimize joins when one of the DataFrames is small     enough to fit in the memory of each worker node. By broadcasting the small DataFrame to all worker nodes, Spark can perform the join operation more efficiently.

**Example Code**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("BroadcastJoinExample").getOrCreate()
large_df = spark.read.parquet("large_data.parquet")
small_df = spark.read.parquet("small_data.parquet")

joined_data = large_df.join(broadcast(small_df), "common_column")

```

### **Potential Issue**

- **Memory Exhaustion**: If the broadcasted DataFrame (**`small_df`**) is too large, it can cause the driver or executor nodes to run out of memory.
    
    **Mitigation Strategies**
    
    1. **Increase Driver Memory**: Allocate more memory to the driver.
        
        ```python
        --conf spark.driver.memory=4g
        ```
        
    2. **Set Broadcast Threshold**: Limit the size of DataFrames that Spark will automatically broadcast.
        
        ```python
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")
        ```
        
    
    These steps help prevent memory issues and ensure efficient execution of broadcast joins.
    

![Screenshot from 2024-06-02 18-03-30.png](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Screenshot_from_2024-06-02_18-03-30.png)

spark.driver.memory → jvm process
spark.driver.memoryoverhead → Non jvm process like object, udf etc
**overheadmemory**:

- **JVM Heap:** The portion of memory allocated to the driver for executing Spark tasks and maintaining the DAG, task scheduler, and other runtime information.
- **Overhead Memory:** Additional memory allocated to the driver for Spark's internal data structures, scheduling information, and other overheads.
    
    ### **How to Handle Driver OOM**
    
    1. **Increase Driver Memory:**
        - Increase the amount of memory allocated to the driver using the **`-driver-memory`** configuration option.
        
        ```
        spark-submit --driver-memory 4g ...
        ```
        
    2. **Increase Driver Overhead Memory:**
        - Allocate more overhead memory using the **`spark.driver.memoryOverhead`** configuration.
        
        ```
        spark.conf.set("spark.driver.memoryOverhead", "1024")
        ```
        
    3. **Optimize Data Shuffling:**
        - Reduce the amount of data shuffled by optimizing transformations and actions to minimize intermediate data sizes.
    4. **Use Efficient Data Formats:**
        - Use columnar data formats like Parquet or ORC that are more memory efficient compared to row-based formats like CSV or JSON.

### **how executor memory(heap memory) is managed in spark.**

![Untitled](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Untitled.png)

So as we see in the above pic executor memory is composed of 3 important memory blocks.
**Reserve Memory**

Reserve memory is a hardcoded value in Spark, set to 300 MiB. This segment of memory is reserved for Spark's internal operations and is not available for user operations or tasks. It ensures that Spark has enough memory to manage its internal processes, which are essential for the stable operation of the application

### **Components of Reserve Memory**

1. **Off-heap Allocations**:
 Memory used outside the Java Virtual Machine (JVM) heap. Off-heap memory can be beneficial for certain workloads because it reduces the pressure on the garbage collector.

**Usage in Spark**: 
Spark uses off-heap memory for tasks that require serialization of objects, storing large data structures that don’t need frequent garbage collection, and handling string internings(String interning is a method where all identical string literals are stored in a common pool). This memory is managed by the operating system (OS) rather than the JVM.
**Configuration**: This can be controlled by settings such as **`spark.memory.offHeap.enabled`** and **`spark.memory.offHeap.size`**.
2. **Garbage Collection Overhead**:
 Memory reserved to handle the process of garbage collection within the JVM. Garbage collection is the process of identifying and discarding objects that are no longer needed to free up memory resources.
**Usage in Spark**: 
Involves managing the lifecycle of objects created during Spark tasks. Frequent garbage collection can pause application threads and affect performance.
**Types of Garbage Collectors**:
    - **Serial GC**: Uses a single thread for garbage collection.
    - **Parallel GC**: Uses multiple threads for parallel garbage collection.
    - **G1 GC (Garbage-First Collector)**: Focuses on managing larger heaps with better performance.
    **Configuration**: Parameters such as **`spark.executor.extraJavaOptions`** can be used to tune garbage collection behavior.
3. **Miscellaneous System Processes**:
 Other internal processes that require memory but do not fall into the main categories of Spark operations.
**Usage in Spark**: This includes memory for logging, monitoring, and managing job execution, which ensures that the application can track and report its status effectively.
**Examples**:
    - **Logging**: Memory used by logging frameworks to record application logs.
    - **Monitoring**: Tools that monitor Spark application performance and health.
    - **Task Management**: Internal management of tasks, stages, and job execution.

### **User Memory in Spark**

**User Memory** is a portion of the allocated executor memory in Spark that is used for various user-defined objects and structures required during the execution of a Spark application. This memory is critical for storing metadata and temporary data structures that support Spark's execution processes. Here’s a breakdown of what User Memory is used for and its key components:
**Characteristics**

- **Percentage**: It constitutes 25% of the allocated executor memory.
- **Purpose**: Used for user-defined objects like hashmaps and UDFs (User-Defined Functions).
- **Limit**: If the objects exceed the allocated limit, an Out Of Memory (OOM) error is thrown. This memory does not spill onto the disk.

**Key Components**

**User-Defined Data Structures**:Custom classes, collections, and other data structures created during the Spark application.
**Example**: HashMaps, lists, or any objects needed for processing logic.

**RDD Partitions**:actual RDD data is stored in Storage Memory, the metadata related to RDD partitions may reside in User Memory.

**Third-Party Libraries**:Data structures from libraries such as MLlib or custom analytics libraries.

**Broadcast Variables**:data of broadcast variables is stored in Storage Memory and metadata  additional data  to these variables may reside in User Memory.

### **Memory Fraction in Spark**

**Memory Fraction** refers to 75% of the allocated executor memory in Spark, which is further divided into **Storage Memory** and **Execution Memory**. This memory is critical for various Spark operations, ensuring efficient data processing and storage.
**Key Components**

1. **Execution Memory
JVM Heap Space**: This is the portion of memory used by Spark for processing data and performing transformations.

**Operations**: Includes shuffle, join, and sort operations. Execution Memory is designed to handle intermediate data generated during these operations.

**Spill to Disk**: If the allocated memory limit is breached, Spark will spill data to disk to manage memory usage. This helps in handling large datasets without running out of memory.
2. **Shuffle Memory**
    
    Data redistribution during groupByKey, reduceByKey, or other shuffling operations.
    
3. **Sort Memory**
    - **Description**: Required for sorting operations like orderBy or sortBy.
4. **Aggregation Memory**
    
    Memory allocated for partial sums or counts during aggregate operations like sum, count, or average.
    
5. **Join Memory**
    - **Description**: Needed for join operations, which may require memory for tasks like hashing, buffering, or sorting the data.

**Storage Memory**
in Spark is part of the program's memory used to store and cache data that is often needed again. This helps Spark run faster by keeping frequently used data easily accessible without having to recompute or reload it.
**Key Components**

1. **Cached Objects**:
    - **RDD Cache**: Stores cached RDD data for quick reuse.
    - **DataFrame Cache**: Caches DataFrames similarly for efficient access.
2. **Broadcast Variables**:
    - **Description**: Stores read-only variables cached on each machine to reduce data transfer overhead.
    
    **Memory Management**
    
    - **Spill to Disk**: When storage memory is full, it spills data to disk to free up space.
    - **Dynamic Boundary**: The boundary between storage and execution memory is flexible, allowing memory to be reallocated between them as needed.
        - **Execution Memory Reallocation**: Execution memory can borrow from storage memory if it runs out.
        - **Storage Memory Reallocation**: Storage memory can use available execution memory space if needed.
        **Configuration
        spark.memory.storageFraction**: This setting configures the fraction of JVM heap space allocated to storage memory.

**when data can be splitted to disk?**
Data can be split to disk in Spark when the allocated storage memory is full. This process is known as "spilling to disk." It happens to free up memory space while ensuring that the computations can continue without running out of memory. Here are the key situations when this occurs:

1. **Cached Data**: If the cached RDDs or DataFrames exceed the available storage memory, Spark will write some of this data to disk.
2. **Broadcast Variables**: When the memory allocated for broadcast variables is insufficient, Spark will spill these to disk.
3. **Execution Memory Pressure**: If execution memory needs more space, it can evict data from storage memory, causing that data to spill to disk.

**when do we get executor oom?**

1. **Large Data**
2. **Expensive Transformations**
3. **Insufficient Memory for Execution**
4. **Large User-Defined Objects**
5. **Insufficient Storage Memory**

## Spark submit and development mode in spark

**`spark-submit`** is a command-line tool provided by Apache Spark to submit applications to a Spark cluster for execution. It handles the deployment of the application, including setting up the driver and executors, configuring resources, and managing the application's lifecycle.

### Key Functions of `spark-submit`:

1. **Submit Applications**: It allows users to submit Spark jobs written in various languages (Scala, Java, Python, R) to a Spark cluster.
2. **Resource Configuration**: Users can specify resources such as memory and CPU for the driver and executors.
3. **Cluster Mode Selection**: Users can choose between different cluster modes like local, standalone, YARN, and Kubernetes.
4. **Configuration Management**: It supports a wide range of configuration options to fine-tune application behavior.
5. **Dependency Management**: Users can include dependencies like JAR files, Python files, and additional configurations.

```sql
#Basic Syntax
spark-submit [options] <app jar | python file> [app arguments]
```

### Commonly Used Options

- `-class CLASS_NAME`: The entry point for the application (required for Java/Scala apps).
- `-master MASTER_URL`: The master URL for the cluster (e.g., `local`, `yarn`, `mesos`, `k8s`).
- `-deploy-mode DEPLOY_MODE`: Deploy mode for the driver (`client` or `cluster`).
- `-num-executors NUM`: Number of executors to launch (for YARN and Kubernetes only).
- `-executor-memory MEM`: Memory per executor (e.g., `2G`, `512M`).
- `-executor-cores NUM`: Number of cores per executor.
- `-driver-memory MEM`: Memory for the driver (e.g., `1G`).
- `-files FILES`: Comma-separated list of files to be placed in the working directory of each executor.
- `-jars JARS`: Comma-separated list of JAR files to include in the classpath.

### Example of Submitting a Spark Application

```sql
spark-submit \
  --class com.example.SparkApp \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 5 \
  --executor-memory 2G \
  --executor-cores 2 \
  --driver-memory 1G \
  path/to/your/application.jar
```

### Diagram: Spark Application Submission with `spark-submit`

```lua

+-----------------------------------------------------------+
|                       spark-submit                        |
|-----------------------------------------------------------|
|  Options:                                                 |
|  --class com.example.SparkApp                             |
|  --master yarn                                            |
|  --deploy-mode cluster                                    |
|  --num-executors 5                                        |
|  --executor-memory 2G                                     |
|  --executor-cores 2                                       |
|  --driver-memory 1G                                       |
|  path/to/your/application.jar                             |
+-----------------------------------------------------------+
                             |
                             |
                    +--------v--------+
                    |     Driver       |
                    | (Cluster Manager)|
                    +--------+--------+
                             |
                             |
      +----------------------+----------------------+
      |                      |                      |
+-----v-----+            +-----v-----+          +-----v-----+
| Executor 1 |          | Executor 2 |          | Executor 3 |
|   (2 cores,|          |   (2 cores,|          |   (2 cores,|
|   2G RAM)  |          |   2G RAM)  |          |   2G RAM)  |
+-----------+            +-----------+          +-----------+

```

### Summary

- **`spark-submit`** is a versatile command-line tool used to submit Spark applications to various cluster managers.
- It allows extensive configuration options to control resource allocation, deployment modes, and application-specific settings.
- Proper use of `spark-submit` ensures efficient execution and management of Spark applications across different cluster environments.

### What is an Edge Node?

An **Edge Node** in Spark is a gateway machine that acts as the point of entry for Spark jobs into the cluster. It allows users to submit their Spark applications and manage them without directly interacting with the cluster's internal nodes.

Key Points about Edge Node:

1. **User Gateway**:
    - Users log in to the Edge Node to submit Spark jobs.
    - It provides a secure access point to the cluster.
2. **Job Submission**:
    - Hosts client tools like `spark-submit` to send jobs to the cluster.
    - Acts as an interface between users and the cluster's resource manager.
3. **Development and Testing**:
    - Users can develop, test, and debug their Spark applications on the Edge Node before deploying them to the cluster.
4. **Resource Management**:
    - Manages the resources and monitors job execution.
    - Does not participate in actual data processing but coordinates with the cluster.

### Roles of an Edge Node:

1. **Client Gateway**: It provides users with access to the cluster. Users submit their jobs from the Edge Node, which then communicates with the cluster's resource manager and nodes.
2. **Job Submission**: It hosts the Spark client, allowing users to submit Spark jobs using `spark-submit`. The Edge Node interacts with the cluster's master node (like YARN ResourceManager or Kubernetes master) to initiate job execution.
3. **Resource Management**: The Edge Node does not participate in the actual data processing but manages and monitors the resources and jobs submitted to the cluster.
4. **Security and Access Control**: It often serves as a secure gateway to the cluster, enforcing security policies and managing user access.
5. **Development and Testing**: Users can develop, test, and debug their Spark applications on the Edge Node before submitting them to the cluster.

Diagram:

Here’s a simple diagram to illustrate the role of an Edge Node in a Spark cluster:

```
plaintextCopy code
+------------------------+
|       User Clients     |
+------------------------+
         |
         | Secure Access (SSH, etc.)
         v
+------------------------+
|       Edge Node        |
|------------------------|
| - Job Submission       |
| - Client Tools         |
| - Monitoring & Logging |
| - Security & Access    |
+------------------------+
         |
         | Submits Jobs to
         v
+------------------------+
|   Cluster Manager      |
| (YARN, Kubernetes, etc.)|
+------------------------+
         |
         | Manages Resources
         v
+---------------------------+
|     Spark Cluster Nodes   |
|---------------------------|
| +-----------------------+ |
| |      Master Node      | |
| +-----------------------+ |
| +-----------------------+ |
| |      Worker Nodes     | |
| | (Executors)           | |
| +-----------------------+ |
+---------------------------+

```

**what if i close my edge node?**

- In **client mode**, shutting down the Edge Node will terminate the running Spark job.
Job Termination
****Loss of Monitoring and Debugging
- In **cluster mode**, running jobs will continue unaffected, but you won't be able to submit new jobs until the Edge Node is back online.
- Regardless of the mode, tools and utilities hosted on the Edge Node will become inaccessible, affecting cluster management, monitoring, and development activities.

### **difference between client mode and cluster mode**

In Apache Spark, there are two main deployment modes for running applications: **Client Mode** and **Cluster Mode**. The choice between these modes affects where the driver program runs and how it interacts with the cluster.

**Client Mode** is a deployment mode where the driver program runs on the machine that initiates the Spark application (typically the user's local machine or an edge node). The executors run on the worker nodes within the cluster.
Characteristics:

1. **Driver Location**:
    - The driver runs on the client machine (local machine or edge node) from which the job is submitted.
2. **Resource Allocation**:
    - Executors are launched on the worker nodes within the cluster.
3. **Communication**:
    - The driver communicates directly with the executors on the worker nodes. This requires a reliable network connection between the client machine and the cluster nodes.
4. **Use Case**:
    - Suitable for interactive workloads or development and debugging scenarios where the user needs immediate feedback and access to logs and monitoring information.

**Cluster Mode** is a deployment mode where the driver program runs on one of the worker nodes in the cluster. The driver process is managed by the cluster manager, and the client machine only initiates the submission of the job.

### Characteristics:

1. **Driver Location**:
    - The driver runs on one of the worker nodes in the cluster, not on the client machine.
2. **Resource Allocation**:
    - Both the driver and executors run on the worker nodes within the cluster.
3. **Communication**:
    - The client machine submits the job and does not maintain a continuous connection with the driver or executors. The driver communicates with the executors internally within the cluster.
4. **Use Case**:
    - Suitable for production workloads or long-running jobs where the client machine does not need to be connected to the cluster after job submission. This mode is more fault-tolerant since the client machine’s connection does not impact job execution.

## What is **Cache vs Persist**

caching and persisting are mechanisms to store DataFrames or RDDs (Resilient Distributed Datasets) in memory or on disk across operations to avoid recomputing them. 
CACHE and PERSIST do the same job to help in retrieving intermediate data used for computation quickly by storing it in memory, while by caching we can store intermediate data used for calculation only in memory , persist additionally offers caching with more options/flexibility. Persist can be thought of as flexible caching.

### Cache

The `cache` method in Spark is a shorthand for the `persist` method with the default storage level, which is MEMORY_ONLY. This means that it stores the RDD or DataFrame only in memory.

Key Points:

- **Storage Level**: MEMORY_ONLY
- **Usage**: When you call `cache` on an RDD or DataFrame, Spark keeps the data in memory on the nodes.
- **Syntax**:
    
    ```python
    val cachedDF = df.cache()
    ```
    
    ```python
    cachedDF = df.cache()
    ```
    
    ### Persist
    
    The `persist` method gives you more control over how the data is stored. You can specify different storage levels to balance between memory usage and fault tolerance.
    
    Storage Levels:
    
    1. **MEMORY_ONLY**: Keeps the data in memory only.Use this when you have enough memory to store all your data and you want the fastest access time.
    2. **MEMORY_AND_DISK**: Stores RDD as deserialized Java objects in memory. If the data does not fit in memory, it spills the data to disk.
    Use this when your data is too big to fit in memory but you still want to keep as much of it in memory as possible for speed.                                                                                 for ex. suppose you have rdd with 500gb so 150gb is stored in ram and rest of 350gb will be stored on disk so it will be a combination.
    3. **MEMORY_ONLY_SER**: Stores RDD as serialized Java objects in memory (more space-efficient but more CPU-intensive to read).
    Stores the data in memory as compressed (serialized) objects, making it more space-efficient but slower to access.
    4. **MEMORY_AND_DISK_SER**: Similar to MEMORY_ONLY_SER but spills to disk if necessary.
    Stores the data in memory as compressed objects, and if it doesn't fit, it stores the extra data on disk.
    5. **DISK_ONLY**: Stores RDD only on disk. 
     Use this when you don't have enough memory to store the data or if the data is too large to fit in memory.
    - Caches data entirely on disk, minimizing memory usage but resulting in slower access times.
    1. **MEMORY_ONLY_2, MEMORY_AND_DISK_2, etc.**: Same as the above but with replication across two nodes for fault tolerance. If one node fails, the data is still available on another node.
    
    Memory can store data in both serialized and de-serialized format , whereas on Disk we can store data only in serialized format
    **serialized** data takes lesser space but more CPU cycles where as **de-serialized** data takes more space but is much quicker for data retrieval
    
    Usage:
    
    - **Syntax**:
        
        ```scala
        val persistedDF = df.persist(StorageLevel.MEMORY_AND_DISK)
        ```
        
        ```python
        from pyspark import StorageLevel
        persistedDF = df.persist(StorageLevel.MEMORY_AND_DISK)
        ```
        
        When to Use Cache vs. Persist
        
        - **Cache**: Use `cache` when you are satisfied with the default storage level (MEMORY_ONLY) and want a simple, easy-to-use method.
        - **Persist**: Use `persist` when you need more control over storage levels, especially when working with large datasets that might not fit into memory.
        
        Example
        
        Consider a scenario where you have a DataFrame with expensive transformations, and you want to reuse it multiple times:
        
        ```python
        from pyspark.sql import SparkSession
        from pyspark import StorageLevel
        
        # Initialize Spark session
        spark = SparkSession.builder.appName("CachePersistExample").getOrCreate()
        
        # Sample DataFrame
        df = spark.range(1000000).withColumn("square", col("id") ** 2)
        
        # Caching the DataFrame
        cachedDF = df.cache()
        
        # Persisting the DataFrame with a specific storage level
        persistedDF = df.persist(StorageLevel.MEMORY_AND_DISK)
        
        # Actions that will trigger caching/persisting
        cachedDF.count()
        persistedDF.count()
        
        # Stopping Spark session
        spark.stop()
        ```
        

## spark dynamic resource allocation

Dynamic Resource Allocation (DRA) is a feature within Apache Spark that enables Spark to **adjust the number of executors** allocated to an application **dynamically**, depending on the workload. Just keep in mind that it can't increase or decrease the **executor's memory**. It can only help in adjusting the **number of executors** dynamically from clusters.

**Key Features of Dynamic Resource Allocation**

1. **Auto-Scaling**: Spark can automatically request more executors when there are pending tasks and release executors when they are no longer needed.
2. **Efficient Resource Utilization**: By scaling resources up and down, Spark ensures that only the necessary resources are used, which helps in optimizing the cost and resource usage.
3. **Improved Performance**: Dynamic allocation can improve the performance of Spark jobs by providing additional resources during peak demand and reducing them when the workload decreases.

### How Dynamic Resource Allocation Works

1. **Initial Setup**: When a Spark application starts, it requests an initial number of executors based on the configuration.
2. **Monitoring**: During the execution of the application, Spark continuously monitors the workload and resource usage.
3. **Adjusting Executors**:
    - **Scaling Up**: If Spark detects that there are pending tasks and the current executors are fully utilized, it requests more executors from the cluster manager.
    - **Scaling Down**: If Spark finds that executors are idle for a specified period, it releases these executors back to the cluster manager to free up resources.

### Enabling Dynamic Resource Allocation

To enable dynamic resource allocation in Spark, you need to set the following configurations in your `spark-submit` command or `spark-defaults.conf` file:

```bash
--conf spark.dynamicAllocation.enabled=true
--conf spark.dynamicAllocation.minExecutors=<min_executors>
--conf spark.dynamicAllocation.maxExecutors=<max_executors>
--conf spark.dynamicAllocation.initialExecutors=<initial_executors>
--conf spark.dynamicAllocation.executorIdleTimeout=<timeout>
```

### Example Configuration

Here is an example configuration for enabling dynamic resource allocation:

```bash
spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=10 \
  --conf spark.dynamicAllocation.initialExecutors=2 \
  --conf spark.dynamicAllocation.executorIdleTimeout=60s \
  /path/to/spark/examples/jars/spark-examples_2.12-3.1.1.jar 100
```

### Configuration Parameters

- **spark.dynamicAllocation.enabled**: Enables or disables dynamic resource allocation.
- **spark.dynamicAllocation.minExecutors**: The minimum number of executors that will be allocated.
- **spark.dynamicAllocation.maxExecutors**: The maximum number of executors that can be allocated.
- **spark.dynamicAllocation.initialExecutors**: The number of executors to allocate at the start of the application.
- **spark.dynamicAllocation.executorIdleTimeout**: The time (in seconds) an executor can be idle before being removed.

### Benefits

- **Cost Efficiency**: By releasing idle executors, you save on resource costs, especially in cloud environments where you pay for the resources you use.
- **Performance Optimization**: By adding more executors when needed, you can improve the performance of your Spark jobs, reducing the overall execution time.
- **Flexibility**: It allows Spark applications to handle varying workloads effectively, scaling resources according to demand.

### Disadvantages of Dynamic Resource Allocation in Spark

1. **Latency in Resource Adjustment:**
Adjusting resources dynamically can introduce latency because it takes time for new executors to be requested, allocated, and initialized. Similarly, there can be a delay in detecting idle executors and deallocating them.
2. **Complex Configuration:**
Misconfigurations can result in under-utilization or over-utilization of resources, leading to inefficiencies and increased costs.
3. **Complexity in Debugging and Monitoring:**
    
    Dynamically changing the number of executors can make debugging and monitoring more complex. The constantly changing resource allocation can make it harder to pinpoint performance issues.
    

## what is partition pruning in spark and dynamic partition pruning

**Partition Pruning** is an optimization technique used in Spark to improve query performance by reducing the amount of data read. It works by eliminating unnecessary partitions early in the query execution process based on filters applied to partition columns.

### Types of Partition Pruning

1. **Static Partition Pruning:**
    - **Definition**: This type of pruning happens at the logical plan optimization stage. The optimizer analyzes the query and determines which partitions can be skipped before any actual data reading occurs.
    - **Example**: If a table is partitioned by the column `year` and the query includes a filter like `WHERE year = 2021`, only the partition(s) corresponding to the year 2021 will be read.
    - **How it works**: During query planning, the optimizer identifies the filter conditions on partition columns and prunes out the partitions that do not match the filter criteria.
2. **Dynamic Partition Pruning:**
    - **Dynamic Partition Pruning** is a technique that allows Spark to dynamically determine which partitions of the large table need to be scanned during query execution.
    - It prunes (or excludes) the partitions that do not match the join condition, reducing the amount of data that needs to be processed.
    
    How It Works:
    
    1. **Query Parsing**:
        - When a join query is executed, Spark looks at the join condition and identifies the partitions that are relevant.
    2. **Partition Pruning**:
        - Instead of scanning all partitions, Spark dynamically prunes away the partitions that do not meet the join condition using the information from the smaller table.
    3. **Execution**:
        - Spark only reads and processes the relevant partitions, making the query execution faster and more efficient.
    
    ### Example:
    
    Imagine you have two tables:
    
    - **Sales**: A large partitioned table with partitions based on `region`.
    - **Regions**: A smaller table containing details about specific regions.
    
    You run a query to find sales data for certain regions:
    
    ```sql
    SELECT *
    FROM Sales s
    JOIN Regions r
    ON s.region_id = r.region_id
    WHERE r.region_name = 'North America'
    
    ```
    
    Without DPP:
    
    - Spark might scan all partitions of the `Sales` table even if only a few of them contain data for `North America`.
    
    With DPP:
    
    - Spark first determines the `region_id` for 'North America' from the `Regions` table.
    - It then prunes away the partitions of the `Sales` table that do not match this `region_id`.
    - This significantly reduces the amount of data scanned and speeds up the query.
    
    ### Benefits:
    
    - **Efficiency**: Reduces the amount of data scanned.
    - **Performance**: Speeds up query execution by avoiding unnecessary reads.
    - **Resource Utilization**: Makes better use of computational resources.
    - **Scalability**: Enhances the ability to handle large datasets efficiently.
    
    ### 
    
    ```sql
    
    SELECT * FROM Sales s JOIN Regions r ON s.region_id = r.region_id 
    WHERE r.region_name = 'North America'
    
       ┌────────────────────────────────────────┐
       │                Regions                 │
       │────────────────────────────────────────│
       │ region_id   | region_name              │
       │─────────────|──────────────────────────│
       │ 1           | North America            │
       │ 2           | Europe                   │
       └────────────────────────────────────────┘
                            │
                            │
                            ▼
                   Determine region_id
                    for 'North America'
                            │
                            │
                            ▼
               ┌────────────────────────┐
               │ region_id = 1          │
               └────────────────────────┘
                            │
                            │
                            ▼
       ┌────────────────────────────────────────┐
       │                 Sales                  │
       │ (Partitioned by region_id)             │
       │────────────────────────────────────────│
       │ Partition 1: region_id = 1             │
       │ Partition 2: region_id = 2             │
       └────────────────────────────────────────┘
                            │
                            │
                            ▼
         Prune Partitions: Only scan region_id = 1
                            │
                            │
                            ▼
               ┌────────────────────────┐
               │       Join Tables      │
               │   On region_id column  │
               └────────────────────────┘
                            │
                            │
                            ▼
               ┌────────────────────────┐
               │   Retrieve Results     │
               └────────────────────────┘
    ```
    

## what is salting and AQE in spark

**1.Salting in Apache Spark** is a technique used to address data skew issues when performing certain operations, particularly joins and aggregations, on large datasets. Data skew occurs when the distribution of data across partitions is highly uneven, causing some partitions to contain significantly more data than others. 
2.Salting is used in Spark to handle data skew, which occurs when the data distribution is uneven across partitions.
3.Salting helps to distribute the data more evenly across partitions, improving the performance and efficiency of Spark jobs.
4.**Salting involves** artificially increasing the number of distinct keys in a dataset by appending a random or unique identifier to each key. This random identifier, known as a “salt,” helps distribute the data more evenly across partitions, reducing the likelihood of data skew.

## what are the ways to remove skewness in spark

### 1. Salting

Salting involves adding a random key to the join key to distribute the data more evenly across partitions.

**How to Implement:**

1. Add a random prefix (salt) to the keys of the skewed dataset.
2. Duplicate the keys in the other dataset to match these prefixes.
3. Perform the join on the salted keys.

### 2. Adaptive Query Execution (AQE)

Spark's Adaptive Query Execution (AQE) can automatically detect and address skewed joins by dynamically optimizing the query plan at runtime.

### 3. Broadcasting

Broadcasting involves sending a small dataset to all worker nodes, reducing the amount of data shuffled across the network. This is particularly effective when joining a large dataset with a much smaller one.

### 4. Repartitioning

Repartitioning the data ensures a more even distribution across partitions.

### 5. Coalesce and Repartition

Use `coalesce` to reduce the number of partitions and `repartition` to increase it, which can help in distributing data more evenly.

The repartition does a full shuffle and create new partitions with data that’s distributed evenly.

Coalesce uses existing partition to minimize the amount of data that’s shuffled. Coalesce may run faster than repartition but unequal sized partitions are generally slower to work with than eqaul size partitions.

Repartition to be faster overall because spark is build to work with equal sized partitions.

Coalesce- it’s is recommended to use it while reducing the number of partitions. for ex if you have 3 partitions and you want to reduce it to 2, coalesce will move the 3rd partition data to partition 1 and 2. partition 1 and 2 will remains in the same container. on other hand, repartition will shuffle data in all the partitions. therefore the network usage between the executors will be high and it will impacts the performance.

Coalesce performs better than repartition while reducing the number of partitions

## What is parquet file format

Parquet is a file format used to store large amounts of data efficiently. It organizes data in a columnar way, meaning data is stored by columns rather than rows. This approach makes it very fast to read specific columns of data, which is useful when you only need a few columns from a large dataset. It also compresses the data well, saving storage space.

![Screenshot from 2024-06-17 15-46-39.png](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Screenshot_from_2024-06-17_15-46-39.png)

**Row vs Columnar storage format**

![Untitled](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Untitled%201.png)

### 1: Row-Based Storage (OLTP)

1. **Row-Based Storage**:
    - Data is stored row by row.
    - In this example, data from Column A, Column B, and Column C are stored sequentially for each row.
    - Efficient for transactional workloads (OLTP) where entire rows are frequently read and written.
    - 
    
    ![Untitled](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Untitled%202.png)
    

### 2: Columnar Storage (OLAP)

1. **Columnar Storage**:
    - Data is stored column by column.
    - In this example, all data from Column A is stored together, followed by all data from Column B, and then Column C.
    - Efficient for analytical workloads (OLAP) where only specific columns are read, reducing I/O and improving query performance.

### Why Do We Need Parquet?

Parquet is beneficial because it:

- **Supports Efficient Storage**: It uses columnar storage which enables better compression and reduces storage requirements.
- **Optimizes Read Performance**: Allows for faster query performance by only reading the required columns.
- **Interoperability**: Compatible with many big data tools like Apache Hive, Apache Impala, Apache Drill, and Apache Spark.
- **Efficient Encoding and Compression**: Reduces the size of the stored data and improves I/O efficiency.

Parquet supports several compression techniques including:

- **Snappy (default)**
- **GZIP**
- **LZO** to reduce storage size.

### Optimizations in Parquet

1. **Compression**:
    - Parquet uses compression algorithms like Snappy, GZIP, and LZO to reduce storage size.
    - Each column chunk can be compressed independently, improving compression efficiency.
2. **Encoding**:
    - Different encoding techniques such as Run-Length Encoding (RLE) and Dictionary Encoding are applied to compress data within pages.
    - Encoding reduces the amount of data that needs to be read and processed.
3. **Column Pruning**:
    - Only the necessary columns are read, reducing I/O operations.
    - This is beneficial when queries only need a subset of columns from a large dataset.
4. **Predicate Pushdown**:
    - Filters (predicates) are pushed down to the storage layer, allowing for early data elimination.
    - Filter data at the storage level.This means only relevant data is read into memory, further optimizing query performance.
    
    ![Screenshot from 2024-06-17 16-22-47.png](Apache%20Spark%20f35a60efa7f44209a2aef75669f6e519/Screenshot_from_2024-06-17_16-22-47.png)
    

### Parquet's Hybrid Storage

Parquet files combine the benefits of both row-based and columnar storage formats. This hybrid approach stores data in a way that allows efficient reading of both entire rows and specific columns.

### How Parquet Organizes Data

1. **Row Groups**:
    - Data is divided into row groups. Each row group contains a subset of rows.
    - This organization allows for efficient data scanning, as entire row groups can be skipped if they don't contain relevant data for a query.
2. **Column Chunks**:
    - Within each row group, data is stored in column chunks.
    - Each column chunk contains data for a specific column across all rows in that row group.
    - This enables columnar compression and efficient columnar reads.
3. **Pages**:
    - Data within each column chunk is further divided into pages.
    - Pages can be of different types such as data pages, dictionary pages, and index pages.
    - This fine-grained structure allows for additional optimizations like dictionary encoding and run-length encoding.

### What is Row Group, Column, and Pages?

- **Row Group**: A row group is a logical horizontal partitioning of the data into rows. Each row group contains column chunks for each column.
- **Column**: Columns in Parquet files are stored together in chunks, allowing for efficient compression and encoding.
- **Pages**: Within a column chunk, data is divided into pages. There are different types of pages like data pages, dictionary pages, and index pages.

### How Projection Processing and Predicate Pushdown Works

- **Projection Processing**: It refers to reading only the necessary columns from the dataset. Since Parquet is a columnar format, it can skip reading columns that are not required, reducing I/O and increasing performance.
- **Predicate Pushdown**: This optimization technique involves applying filter conditions as early as possible, ideally at the data source level. With Parquet, the filters are pushed down to the storage layer, which can significantly reduce the amount of data read and processed.

## What is partitionBy and Bucketing by in Spark

### Partitioning by in Spark

**Partitioning** is a way to divide data into smaller, manageable pieces based on a column or set of columns. This is especially useful when working with large datasets, as it can improve the efficiency of data processing and querying by reducing the amount of data read and processed.

### How Partitioning Works

- **Partition Columns**: When you partition a DataFrame, you specify one or more columns to partition by. Each unique value in these columns creates a separate directory, and the data corresponding to that value is stored in that directory.
- **Optimization**: When queries filter on the partition columns, Spark can skip reading entire partitions that do not match the filter criteria, reducing I/O and speeding up the query.

### Example

```python

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PartitioningExample").getOrCreate()

# Create a DataFrame
data = [("USA", "New York", 1), ("USA", "California", 2), ("Canada", "Ontario", 3)]
columns = ["Country", "State", "Count"]
df = spark.createDataFrame(data, columns)

# Write the DataFrame partitioned by "Country"
df.write.partitionBy("Country").parquet("path/to/output")
```

In this example, the data will be stored in directories like `Country=USA` and `Country=Canada`.

### Bucketing by in Spark

**Bucketing** is another technique used to organize data, but instead of creating separate directories for each unique value, bucketing distributes data into a fixed number of buckets (files) based on the hash of a specified column. This technique is particularly useful for optimizing join operations.

### How Bucketing Works

- **Buckets**: When you bucket a DataFrame, you specify the number of buckets and the column to bucket by. Spark assigns each row to a bucket based on the hash value of the bucket column.
- **Join Optimization**: Bucketing helps with join optimizations because if both DataFrames being joined are bucketed by the same column and have the same number of buckets, Spark can perform the join more efficiently.

### Example

```python
# Create a DataFrame
data = [("John", 1), ("Jane", 2), ("Joe", 3)]
columns = ["Name", "ID"]
df = spark.createDataFrame(data, columns)

# Write the DataFrame bucketed by "ID" into 4 buckets
df.write.bucketBy(4, "ID").saveAsTable("bucketed_table")
```

In this example, the data will be distributed into 4 buckets based on the hash of the `ID` column.

### Key Differences

- **Partitioning**:
    - Creates directories for each unique value of the partition column(s).
    - Best suited for scenarios where queries filter on the partition column(s).
    - Helps in skipping entire partitions, reducing I/O.
- **Bucketing**:
    - Divides data into a fixed number of buckets (files) based on the hash of a column.
    - Useful for optimizing join operations.
    - Does not create separate directories but distributes data within files.

### When to Use Each

- **Partitioning**:
    - Use when you frequently filter your data by certain columns.
    - Effective for large datasets where skipping partitions can significantly reduce the amount of data read.
    - Commonly used in ETL processes and data warehousing.
- **Bucketing**:
    - Use when you frequently perform joins on a specific column.
    - Effective for large datasets where joins can be optimized by evenly distributing data across buckets.
    - Commonly used in scenarios requiring efficient join operations.