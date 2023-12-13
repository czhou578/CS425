# MP2

## Instructions

First, run the server file on any VM host using

```
python server.py [flag]
```

Flag can be any of the following

- "-list_mem" for listing the machine's membership list,
- -list_self is to list the id of the joined machine,
- -join is to let the machine join the system
- -leave is to let the machine voluntarily leave the system
- -susp is to enable suspicion with gossip
- -dis_susp is to disable suspicion with gossip

The "vm.log" file will be generated and can be used to debug!

**Note that VM3 is the introducer machine**
