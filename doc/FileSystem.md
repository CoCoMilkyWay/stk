# 文件系统配置说明

## 项目概述

本项目基于A股L2数据,数据储存单元:每日行情(单标的 快照+逐笔成交+逐笔委托),总文件数在数千万级别。

## 配置要求

请使用高速SSD作为数据储存:

- **分区方法**: GPT(不选MBR, APM)
- **文件系统**: XFS(不选NTFS, EXT4, Btrfs, F2FS)
- **IO调度**: Noop(不选Deadline, 不选CFQ)
- **挂载参数**: 特殊优化(这些参数是session only的,不改变存储数据和文件系统本身,重载后失效)

**注意事项:**
- 下面为wsl在windows下的flow(用来调试), 纯linux环境(用来生产)可以参考, 配置更简单
- 对于m.2的机器,用fstab持久化挂载
- 对于usb测试机器,每次需要手动挂载
- 记得每次用ssd前检查挂载状态,不要把数据写到wsl的虚拟盘里

**查看设备状态:**
```bash
lsblk -o NAME,MODEL,SIZE,ROTA,TRAN,FSTYPE,MOUNTPOINT
```

---

## Windows PowerShell 配置(管理员权限)

### 1. 查看磁盘信息

```powershell
Get-Disk | Format-Table Number, FriendlyName, SerialNumber, BusType, PartitionStyle, Size
```

输出示例:
```
Number FriendlyName             SerialNumber                             BusType PartitionStyle          Size
------ ------------             ------------                             ------- --------------          ----
     0 WDC WDS100T2B0C-00PXH0   E823_8FA6_BF53_0001_001B_448B_48E3_33E0. NVMe    GPT            1000204886016
     1 Samsung SSD 9100 PRO 4TB 0000000000000000                         USB     GPT            4000787030016
```

### 2. 挂载磁盘到WSL

```powershell
$disk_id = 1
wsl --shutdown
# 裸盘透传(不让 Windows 挂载分区),这样进 WSL 里才能分区/格式化
wsl --mount "\\.\PHYSICALDRIVE$disk_id" --bare
```

---

## WSL2 Ubuntu 配置

### 1. 找到磁盘设备名

一般是 `/dev/sda` 或 `/dev/sdb`

```bash
lsblk -o NAME,MODEL,SIZE,ROTA,TRAN,FSTYPE,MOUNTPOINT
```

输出示例:
```
NAME   MODEL              SIZE ROTA TRAN FSTYPE MOUNTPOINT
sda    Virtual Disk     388.4M    1      ext4
sdb    Virtual Disk       186M    1      ext4
sdc    Virtual Disk         4G    1      swap   [SWAP]
sdd    Virtual Disk         1T    1      ext4   /mnt/wslg/distro
sde    SSD 9100 PRO 4TB   3.6T    0
└─sde1                    3.6T    0      xfs    /mnt/dev/sde
```

### 2. 定义设备变量

```bash
DEV=/dev/sde
```

### 3. 创建 GPT 分区表

```bash
sudo parted $DEV --script mklabel gpt
```

### 4. 创建主分区

建一个覆盖全盘的主分区(从 1MiB 对齐)

```bash
sudo parted $DEV --script unit s print
```

输出示例:
```
Model: Samsung SSD 9100 PRO 4TB (scsi)
Disk /dev/sde: 7814037168s
Sector size (logical/physical): 512B/512B
Partition Table: gpt
Disk Flags:
Number  Start   End          Size         File system  Name     Flags
 1      65535s  7814000189s  7813934655s               primary
```

### 5. 定义分区名

```bash
PART=${DEV}1
```

### 6. 格式化 XFS

带高级调优(需要安装:`sudo apt install xfsprogs`)

```bash
sudo mkfs.xfs -m bigtime=1 -n size=8192 -i size=512 -d agcount=256 $PART
```

输出示例:
```
meta-data=/dev/sde1              isize=512    agcount=256, agsize=3815398 blks
         =                       sectsz=512   attr=2, projid32bit=1
         =                       crc=1        finobt=1, sparse=1, rmapbt=1
         =                       reflink=1    bigtime=1 inobtcount=1 nrext64=0
data     =                       bsize=4096   blocks=976741831, imaxpct=5
         =                       sunit=0      swidth=0 blks
naming   =version 2              bsize=8192   ascii-ci=0, ftype=1
log      =internal log           bsize=4096   blocks=476924, version=2
         =                       sectsz=512   sunit=0 blks, lazy-count=1
realtime =none                   extsz=4096   blocks=0, rtextents=0
```

### 7. 挂载文件系统

```bash
sudo mkdir -p /mnt/$DEV
sudo mount -o noatime,nodiratime,attr2,inode64,logbufs=8,logbsize=32k $PART /mnt/$DEV
```

**挂载参数说明:**
- `noatime,nodiratime`: 避免每次访问更新时间戳,减少写 I/O
- `attr2`: 优化扩展属性存储
- `inode64`: 大盘随机分布 inode,减少 AG 内热点
- `logbufs=8`: 日志缓冲区数量,提高并发写入性能
- `logbsize=32k`: 日志块增大,减少日志写入频率

### 8. 配置 I/O 调度器

```bash
DEV_NAME=$(basename $DEV)
echo none | sudo tee /sys/block/$DEV_NAME/queue/scheduler
```

### 9. 验证配置

```bash
mount | grep /mnt/$DEV
df -h /mnt/$DEV
xfs_info /mnt/$DEV | sed -n '1,25p'
```

**注意:** 持久化挂载(fstab)对于USB用处不大,不考虑。

---

## 其他工具

### rsync 文件同步

```bash
rsync -av --progress --info=progress2 /source/folder/ /destination/folder/
```
