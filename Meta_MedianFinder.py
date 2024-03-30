import heapq

class MedianFinder:
    def __init__(self):
        # Max heap for the left half
        self.max_heap = []
        # Min heap for the right half
        self.min_heap = []

    def add_number(self, num):
        # Add the number to one of the heaps
        if not self.max_heap or -self.max_heap[0] >= num:
            heapq.heappush(self.max_heap, -num)
        else:
            heapq.heappush(self.min_heap, num)
        print('before', self.min_heap, self.max_heap)
        # Balance the heaps
        if len(self.max_heap) > len(self.min_heap) + 1:
            heapq.heappush(self.min_heap, -heapq.heappop(self.max_heap))
        elif len(self.min_heap) > len(self.max_heap):
            heapq.heappush(self.max_heap, -heapq.heappop(self.min_heap))
        print('after', self.min_heap, self.max_heap)

    def find_median(self):
        print(self.min_heap, self.max_heap)
        # Calculate the median based on the two heaps
        if len(self.max_heap) == len(self.min_heap):
            return (-self.max_heap[0] + self.min_heap[0]) / 2.0
        else:
            return -self.max_heap[0]

# Example usage
median_finder = MedianFinder()
median_finder.add_number(5)
median_finder.add_number(1)
median_finder.add_number(9)
median_finder.add_number(7)
# median_finder.add_number(4)
# median_finder.add_number(5)
# median_finder.add_number(7)

print("Median:", median_finder.find_median())
