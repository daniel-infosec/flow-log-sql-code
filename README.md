# README

Code released as part of fwd:cloudsec 2022

Code is released as is and is meant to be for educational purposes for security practioners. If you intend to use this in your production environment, it won't work out of the box.

# Bias

These sql files are intended to help analyze your cloud and flow logs for the purposes of identifying malicious behavior in your enviornment. At the time of publication, I work at Snowflake which colors the way in which I digest and present information, including this code. So there may be some Snowflake-isms that appear in this code.

This means I use bracket notation to access arrays in SQL since I came from a Python background :)

# How to Use

There's 3 primary folders broken down by each of the major Cloud Service Providers (CSPs). Each primary has 3 sub-folders.

## Entities

These queries are designed to gather the entities we'll be using to later join on our flow logs. For AWS, this is ENIs and EC2 instances. For GCP, this is VMs. For Azure, it's VMs and NICs (with their IPs).

## Flow Logs

In this folder, we build a view of our flow logs for each CSP and then join that on the entities we built above. For some of these entities, such as ENI in AWS, it's a straight join. For others, such as Azure, we have to join on IPs. If you're in an organization with only a few hosts or tons of subnets, this may be straightforward. However, we have a large number of internal hosts that we recycle frequently at Snowflake. This necessitates joining not only on IP address but also ensuring our flow log timestamps line up with the time when the entity existed. This yields imperfect matches, but it's the best solution we've found at this time.

### Entities

We use AWS Config, GCP asset inventory, and SnowAlert (in addition to some other vendors) to collect information on what resources we have into our databases. My goal in this repo is to present free options for your use. I'm sure there's tons of other solutions from other vendors (Snowflake support in cloudquery when? :P) that would work. You'll just have to your flow log and entities schemas (and potentially other downstream queries).

We also assume you're using tags to categorize the purpose of your virtual machines and other resources. If you're not, these queries will not work out of the box. You'll have to identify how you know what purpose a VM has and then adjust the queries for that identifier.

## Security Queries

In this folder I provide some starter queries to search your flow logs for IOCs, beaconing, port scans, and allowlist rules based on instance role.

## Performance

You know that Parks and Recs meme where Ron says bring him ALL the bacon they have? At Snowflake, we're like that with logs. So we have a LOT of flow logs. Last I checked it's many TB per day. We also have a lot of compute nodes (VMs/instances/containers/etc). This means when we try and do some complex joins, we can run into performance issues. To resolve this, we have the following tricks.

1. Materialize some views into tables. This is useful if the view will be comptued multiple times. Recomputing a view can be expensive - querying a table is cheaper.
2. Timebound queries. In general we'll timebound our queries/views (7 days, 30 days, 180 days, etc). We'll then label the views as such (e.g. AWS_VMS_30D, AWS_VMS_180D, etc) so that engineers know what's inside and can choose the right one for the analysis they're doing. Having fewer rows in joins can improve performance.
3. Aggressively filter. Maybe you don't need EVERY Azure flow log in the join. Maybe you want to analyze just internal to external or just accepted connections or just queries involving your HTTP servers. Building out views for these use cases can be helpful. If you write a query and you think someone else would want to use it one day, turn it into a view.

# Universal Flow Log

This query attempts to union our AWS, Azure, and GCP flow logs. I wouldn't recommend using this. It's meant to illustrate the vast differences between each of these CSPs in terms of how they present their flow logs.

# Other Resources

## DBT

We use DBT (https://www.getdbt.com/) to keep our queries in our code repos and deploy them to our DBs. While Snowflake has an established partnership with DBT (bias alert), I personally love the technology and can't imagine using SQL in a production environment without it.

# FAQs

## Why SQL?

I work at Snowflake. It's kind of our thing.

## What should I buy?

I'm not a sales guy and I strive to include free options whenever I write (with biases and exceptions noted). Come find me at fwd:cloudsec or DM me if you want opinions.

## Why Isn't the Code Working?

Feel free to shoot me a DM and I'll try and help you out. I tested this as much as I could but I had to rewrite most of it so may have made some mistakes at parts.

## How Are Your Cats so Awesome?

They take after their owner ;) my wife
