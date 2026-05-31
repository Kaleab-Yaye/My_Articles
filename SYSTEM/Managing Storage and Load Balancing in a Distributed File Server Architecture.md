In my last article, I talked about how we can gauge the amount of GET requests that are hitting a file server, and also how many times a specific file was requested. In this article, I will be talking about another issue and how I got around it. 

The two problems we are tackling are:
1. Removing the least requested file from the file server to save storage.
2. Identifying the most requested file on a server, so that load balancers can distribute hot files across other servers.

# Tracking the Least Requested File 

The project that I am working on involves a Node server downloading a file from S3 storage, with the sole purpose of streaming it. Now, the problem is as it downloads more files from S3, its storage will fill up. 

One way to manage this is, after free space gets low, we can make th seerver not download anymore files and make it stay just streaming what it already has. This is not ideal because we need to take advantage of all streaming Nodes; a server could hold onto a dead file that is not being downloaded anymore for a long time instead of kicking it out and serving another hot file. And also, where is the fun in that?

The other working solution that we have is to create a way to track the least requested file, and when storage starts filling up: we kick it out. To do this, I chose a doubly linked list with some modifications to it.

## The List

When a file gets downloaded from the S3 storage, the Node server creates a `Node` object in memory that looks something like this:
```c++
struct Node {
    Node* prev;
    Node* next;
    UUID fileId;
};
```
And then it links those nodes together in a doubly linked list. 
This list will have a global `head` pointer pointing to the top Node in the list, and a `tail` pointer pointing to the end of the list.

Now we will also have another structure: a `hashMap`. 
This map will store the name of the file as a key and the pointer to the node that corresponds to this file name in the list as the value. The structure will look something like this:

<img width="1941" height="810" alt="image" src="https://github.com/user-attachments/assets/3234ba92-1c0f-42b3-8507-8ea08e223d70" />

Now let me show you how file-serving threads will use this DLL (Doubly Linked List) + Map to put the file that they are requesting to the top of the list.
1. A request to get bits of file comes to the server; a thread will take responsibility and starts the IO process.
2. Asynchronously, the thread goes to the map and finds the pointer to the designated Node.
3. It removes the Node from the list and updates the pointers of the nodes to the left and the right.
4. It puts the node at the top of the list, after updating the pointers of the Node that was at the top.

This way, the target Node will be the top Node. The thread updates 8 pointers.

As this happens over and over, the least requested files will sink down as the most requested ones will bubble up.

There are two major function-breaking edge cases here:

### Concurrent Threads

Update to the DLL is happening many times a second. it Because of the fact that not only will one file get requested many times (possibly concurrently), but also all the files in the list get requested many times (possibly concurrently). So, we could be looking at thousands of threads trying to update the same single DLL at the same time, which will eventually leave the DLL in a broken state.

One approach to solve this is to lock the entire DLL when one thread is working on it. Locking is not bad, I will even use it in the optimal solution, but the problem is, as you lock the DLL, all the threads will line up. If you have a spike in traffic, the number of threads waiting for the lock, in the queue, could grow too much, to the point where one thread could get access to the lock up to seconds after its initial request.

So, instead of a naive lock implementation, I tried to solve the problem with a `ring buffer` and one non-ephemeral working thread that is the only thread allowed to touch the DLL.
The ring buffer will be an array of:
```c++
struct Work {
    Status stat;
    Node* target_node;
};
```
`Status` for now is an enum with four states: `DONE`, `WORKING`, `EMPTY`, and `OPEN`. The program implementation of this could have many addtional states, like `FAILED`, to show the working thread couldn't do the work for any reason.

It will also have two variables:
1. `read_index`: This one is used by the traversing working thread to complete work.
2. `write_index`: This one is mutex-lockable and is used by the file-serving threads to queue their work.

Now let's trace what happens from the moment a new request comes through up to the moment the DLL is updated:
1. An ephemeral thread receives a request and starts file serving.
2. Asynchronously, it holds a lock on the `write_index`.
3. It checks if the flag on that index is not `WORKING` or `OPEN`. If it is, it means the buffer is full. It just exits and serves the file.
4. If it is not, it updates the flag to `WORKING` and pastes the pointer value of the Node it is supposed to work on into `target_node`.
5. If it is, It updates the flag to `OPEN` and exits, after incrementing the `write_index` by one.

6. The working thread is constantly looping the array; when it reaches this newly updated bucket, it will see the flag `OPEN`.
7. It extracts the pointer -> goes to the DLL -> puts the target node on top.
8. It updates the flag to `DONE` and moves to the next index.

Now you might be confused with step 3 and say, "Aren't we losing updated information?" And yes, we are. But the thing is, if the buffer is full, there is a good chance the hot file is already on the top, so dropping this update won't hurt.

And also, for some reason like an unfairly put-to-sleep thread, the working thread and other threads could complete their work while one thread is still in a `WORKING` state. When this happens, we call it `thread lapping`. But it is not a problem for our architecture, as the working thread only works on a bucket if the flag in it is `OPEN`. If the flag is `WORKING` or `EMPTY` (at the start), it just stops at that index and waits until it becomes `OPEN`. This avoids the lapping edge case.

### Thrashing

Well, what we just built is well-protected from concurrency bugs, so we could trust that the least requested file or files will sink down. And when the node wants to remove a file, it could just go to the tail, remove the Node (from memory), and the file associated with it (from disk).

But here is the problem: what if the file was being requested by a user, even though it had low traffic compared to other files (it could still be an alive file)? If we remove this file just because it is at the bottom, when the user requests to get the file, they will be hit with a 404. 
Or, if our server handles this failure safely, the server will be forced to download the file again from the s3 storage, which its node will again fall to the bottom of the DLL and get deleted again. This constant downloading and deleting of the file is called `THRASHING`, and we want to avoid this.

But the good news is we don't have to build another complex system just to avoid this. We will instead use the `Sliding Window` implementation that I explained in the last article.

What will happen is that when the server tries to remove a Node from the DLL, it must go to the sliding window of the file and see if any requests happened in the past 1 minute. If not, it means that it is a safe bet that removing it won't cause thrashing, this is because:
1. The file is already at the bottom of the list.
2. There were no requests made to it in the past 1 minute.

By implementing this, we have created a thrashing-resilient way of identifying the least requested file for removal when storage fills up.


# Identifying the Most Requested File(ish)

you Probably have noticed so far that the DLL can be used for two purposes. One which we have seen (to find the least requested file), and the other one which is for finding frequently requested files.

One of the reasons why we would need to know a frequently requested file in a distributed streaming infrastructure, where many Node servers are serving files, is for load balancing. If you know the highly requested file in a currently busy server, you can share that file across other relatively free node servers, and then alleviate the load on the busy server by routing traffic to those Nodes.

Now, as I have said, we have already built a data structure that bubbles up the hot files to the top. If a file is too hot, chances are most of the time if you look at the top few nodes, it will be among them. With that in mind, this is how I implemented the solution:

When the central server requests the hottest file, we choose how many of the top nodes we want to look into. For now, I am going to choose to iterate over 100 nodes. In your implementation, this could be 1000 nodes. 
Then, the worker thread that updates the linked list is stopped from updating the list while we are traversing the DLL. 
Note: Since only one thread was allowed to update the linked list, you can see how easy it is to stop it from updating the list without complicated lock scenarios; that we would have had to deal with if all ephemeral threads were allowed to touch the DLL. 

While traversing the top 100 elements, we also go to the sliding window that we built in the last article (again), sum up the number of requests in the past 1 minute for that specific Node (file), and then keep track of the largest of the 100 as we traverse, and return the file name or Id.

As you can see, we are traversing 100 nodes + 60 buckets of the sliding window. That is around 6000 operations. I was thinking of making it more optimal than this, but it would be overengineering because:
6000 operations is not that big for a CPU, when put into the context that this doesn't happen constantly; it only happens when we need to know the most frequently requested file.

The question you could raise here is: How sure are we that the hottest file is at the top of the list? The answer to that is we can't be 100% sure, and we don't need to be, as the DLL is getting updated constantly. 

However, since a hot file is a file that is getting requested more times than other files, chances are it will be on top. And even if we miss it from the top 100, we will find the next top hot files. Even if we miss those, the central server will get a response anyway. And even if that file is not the hottest file from the list, it will still be distributed across servers, lowering the load on the busy server. This will repeat until the busy server is back to a normal load.

The beauty of this architecture is we used pretty minimal data structures, and each is used more than once:

1. **Sliding Window:**
   Was used in avoiding Thrashing when a file is removed, and also was used in finding the most frequently requested file.

2. **DLL:**
   Was used to track the least and most frequently requested files all at once.

It is with this implementation that I got around those issues.
