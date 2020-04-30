BoundedOutOfOrdernessTimestampExtractor 类

此函数要求输入一个时间间隔，在输入的stream中周期性地产生timestamp，即使某些events在watermark后面到达，只要这些events的时间戳与该watermark的时间戳之差小于这个时间间隔，则这些events会与在该watermark之前的那些events在一个window里进行计算。
注意：通过这个例子可以发现几点有意思的现象：

(1) watermark的时间戳不一定是该watermark被插入stream的时间,watermark的时间戳是触发计时器用的，所以不一定非要是watermark本身的时间。

(2) 多个watermark的时间戳可能相同

![MyNewWatermark](../../../../../../resources/img/16236.png)