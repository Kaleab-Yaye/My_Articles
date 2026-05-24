I was working on this problem that is interesting, and thought to share it.

So the challenge goes like this: a certain server is streaming bits of data (it is serving files) to multiple endpoints. Another central server will constantly ask this node for the total traffic it has been hit with and also the number of times a certain file it was serving was requested. The solution is generally geared to solving the second problem, as the implementation for the first problemw, hich is finding the total traffic that hit the server in the past one minut, is almost identical to solving the issue with finding the number of requests for a given file.

At first, the problem might seem easy; it still is, but not as easy as I assumed it would be at first.

So here is how I eventually reached the optimal solution.

## You Can't Just Store Everything

In this problem, you can't just use a counter that increments on every request. This is because we don't need elephant memory; we just need to know the number of requests that happened within the past minute.

## You Can't Have a Counter That Resets

After ruling out a counter that continuously increments, you could logically say, "Why not reset the counter every minute?" This way, it will only hold the number of hits within that minute. This might seem like a solution, but the problem is that the server that is asking for the hit data (we will call it the central server) and the server that is serving the data (node server) must be synchronized down to a second. What I mean by that is if the central server's request is a second late due to latency or some other issue, the counter will be reset. The data the central server gets will be corrupted. And also, managing thousands or potentially hundred thousands of counters, and the loop that resets them every minute, is just poor optimization.

## What the Optimal Solution Should Look Like

If a central server asks the node server for hit data at 2 minutes and 53 seconds or 2 minutes and 54 seconds after booting up, it should always get the correct data within that minute. And it must be done in a way that is forgiving to compute resources.

So the textbook solution for these types of problems is the Sliding Window Counter (an array-based one).

The idea is you have an array with 60 buckets, each representing one second in a minute. When a request for a file hits the node server, the thread handling the request will use a given pointer to increment the number of hits in that bucket. The good thing about this implementation is the central server can ask the node server for the data at any moment, and it will get guaranteed correct data for a span of a minute.
<img width="2169" height="725" alt="sliding_windows" src="https://github.com/user-attachments/assets/17bd4196-5347-49cc-9d50-ac5a547be334" />

But how does the pointer move to the correct second bucket?

There are two implementations for this, one of which we will use.

### The Iterating Pointer

The first and easy implementation of this is to make the pointer move to the right each second. To do this, you would have a loop that moves the pointer every second. When a pointer moves to a new bucket, it erases or resets the hit in that bucket to 0. This works fine if you are managing a few files, but the problem is when you are working with a huge number of files, creating a loop for each file will take a toll on the CPU cycles; it is expensive. You might be tempted to also assume to just create one thread-safe pointer (really an index for a bucket) that will be used as an index pointer for all arrays we have. The problem with this, however, is that clearing expired buckets won't happen naturally. With a loop for a single array implementation, you can program the loop to clear the next bucket before moving the index pointer; however, with one thread-safe index pointer variable, you have to manage a list of arrays and then iterate over each one to do the same, which is a performance killer.

### Using Unix Timestamps

The second implementation is using Unix timestamps. For the reasons we will see next, this is a smart and optimal implementation for the specific sliding window we are working on. The Google definition for a Unix timestamp is: "a system for tracking time represented as a single number of seconds that have elapsed since a fixed starting point". When you get the Unix timestamp, it will look something like this: `1715500000`, and the next second it will be `1715500001`.

### Using Modulo 60 for the Sliding Window

But how is this useful to us? As it is, it is not, but we use the modulo of 60.

The key is the modulo of any number equal to or greater than 60 will always be `0 <= n <= 59`. And if you are with me so far, we have said that our sliding window array also has the same range.

So when a thread wants to increment the right bucket's hit data, it will just do simple math like `get_Unix_Time_Stamp() % 60` to identify the correct index it should be working on.


## Edge cases with Unix Timestamp implementation

There are edge cases that come with this implementation as well; let's address them one at a time.

### Identifying the Correct Bucket and Handling Idle Time

Unlike the loop implementation, there is no reset of data every time the pointer moves to the right; there is really no pointer, to be precise. So when a thread calculates the right index, it should know if the current hit count in that bucket belongs to the current instant second or is from the past.

The other issue is a file could be requested a few times in a given second and not get requested for some time. When the file gets re-requested eventually, we run into some weird bugs. Let me show you what I mean with a solid example:

- Let's assume there are requests made to get a single file at the timestamp `1715500000`.
- The designated thread will do the math and find that this falls into index 40, which is the 41st second.
- Now at index 40, there is the number 1 to indicate the hits that happened in the 41st second.
- Assume for 8 seconds the file is not requested, but in the 9th second, a file gets requested; the timestamp will be `1715500009`.
- The designated thread will also do the math and find out that this corresponds to index 49, which is the 50th second.
- It will update the hit to 1. Now, notice that buckets `[41...48]` are not updated and are still holding the old expired data

<img width="2068" height="417" alt="Unix_time_stamp" src="https://github.com/user-attachments/assets/6c3c870a-f167-49ca-bd4c-a9cc8c86edb0" />


When the central server asks for hit data, the node can't just add all the numbers in this array because old data that was not overwritten will break the data correctness. So the solution for this edge case is: instead of storing a single integer value, you store a structure that couples the timestamp and the number of hits.

```rust
struct hit_data {
    time_stamp: int64
    hit_count: int32
}
```

Now when the central server asks for hit data, the node gets the `current_time_stamp`.

It loops over the 60 buckets, comparing the `time_stamp` of each struct in each bucket to the `current_time_stamp` with the logic: if `current_time_stamp - time_stamp > 60` or `< 0`, don't add it to the final result. With this, the data can't get corrupted with expired data.

### Preventing Concurrency Bugs During Writes

The thing is, multiple threads will be working on the same file at the same time, and some of those threads could try to update the hit data at the same time; this is a problem. When the CPU manipulates the data on a given variable, in its attempt to be optimal, it doesn't update the value present in memory until later on when it decides it is a perfect time to; instead, it holds on to the updated value in its local cache. When you have multiple threads manipulating a single variable, you run into a classic concurrency bug: overwritten updates.

To avoid this, the hit count variable must be ATOMIC. An atomic data type gets flushed to memory the moment it gets updated.

So our update structure is:

```rust
struct hit_data {
    time_stamp: int64
    hit_count: AtomicI64
}
```

### The Read-While-Write Problem

When the central server asks for the hit data and the loop starts summing the values, at the same moment there is a good chance another request could be made to pull bits of the file. When this happens, the designated thread will try to update (increment) the `hit_count` of one of the indices. So again, if this is let to happen, the central server will get corrupted data due to a subtle read-while-write bug. To fix this, what I did was to make sure the sliding window array is inside a mutex-locked struct. This struct gets locked before the loop starts and gets unlocked when the loop terminates. The implementation is highly language-dependent, but almost all modern programming languages let you use mutex locks.

And that is how I tackled this specific problem I was facing in my high-volume, data-intensive project. 
