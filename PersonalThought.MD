To the Reviewer,

My career goal is to become a quantitative software developer. I am very interested in low-level performance optimization and trading infrastructure design. I also enjoy helping traders and researchers build research models and developing analytic tools to monitor performance and risk.

This project is extremely interesting to me, and I really enjoyed working on it.

After reading the requirements, I thought about what kind of application would be easy for others to understand, run, and review. The idea that came to my mind was an internal dashboard for traders or researchers to replay MBO market data. Based on this idea, I designed the system in this way.

At the beginning, I was not sure whether I should directly stream the DBN data format. To make the system more straightforward and easier to reason about, I decided to first convert the data into a CSV file. This helped simplify the data flow and made debugging and validation easier.

One part that was initially confusing to me was the definition of a “message” in the streaming requirement. I decided to treat each individual event (one MBO record) as one message. Based on this design, the TCP rate selector in the frontend controls how many event messages the streamer sends per second.

The provided dataset contains a total of 38,212 events, so the server is always able to ingest all messages without backpressure. The different rates (200, 1,000, 10,000, etc.) mainly reflect the streamer’s sending speed rather than a limitation of the server itself.

In the past, I have worked on personal projects where I built an order book matching engine that ingests data from binary files. Through this experience, I learned that efficient order book updates (for example, O(1) operations where possible) are very important for real trading systems.

For this project, my main goal was to demonstrate that I can build a trading-related system from scratch. I worked across the frontend, backend, and database layers, and I containerized the application using Docker. I also have experience with CI/CD concepts and infrastructure-as-code, such as AWS CloudFormation, and I am familiar with using cloud services (for example, AWS and EKS) to improve scalability and resilience.

I also have experience with multi-threaded programming, including understanding hot paths versus cold paths, synchronization, and measuring end-to-end latency. I have spent time debugging performance and latency issues in complex systems.

I am aware that I currently have limited experience with writing unit tests for real-world systems and with advanced memory tuning. However, I am very willing to learn and improve in these areas. I am a fast learner and enjoy picking up new skills when facing real engineering problems.

I will continue to improve myself as an engineer.

Yuming Wang