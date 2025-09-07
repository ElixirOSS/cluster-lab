# ClusterLab

This is a basic Phoenix application that demonstrates automatic clustering with
[LibCluster Dynamic SRV](https://github.com/ElixirOSS/libcluster-dynamic-srv).
This lab uses Consul for DNS discovery using SRV records. It could probably be
reused with any service mesh that supports SRV DNS discovery.

## Starting the Lab

The lab uses a [Taskfile](https://taskfile.dev/) to aid in running things. To
ensure you have all of the necessary tools, there is a Brewfile in the root
directory to help with this if you are on OSX.

**Install CLI Tools**

* `brew bundle`

**Run The Lab**

* `task up`

It will take a few minutes to build the Docker images upon initial startup, but
once they are built, the lab should be ready to use.

You can access the Consul UI at http://localhost:8500/. There isn't anything
needed to do in Consul, but it is there for you to explore. This lab uses
[docker-consul-agent](https://github.com/zenchild/docker-consul-agent) to add
new services to Consul using Docker labels in the `docker-compose.yml` file.

## Verify the Cluster

Once the lab is up and running, you can verify that the cluster is working by
running the following command:

* `task iex_a`

This will drop you into an IEx shell on node_a. You can then run the following
command to verify that the cluster is working:

* `Node.list()`

## Playing with the Cluster

There is a basic leader election example in the
[lib/cluster_lab/topology.ex](lib/cluster_lab/topology.ex) file. It uses a
Rendezvous Hash to choose the leader and is inspired by Bryan Hunter's talk on
[Waterpark](https://youtu.be/hdBm4K-vvt0?si=7IU2vLBixi7WiAeu). I'm sure there
are pleanty of things wrong with it, but it's fun to play with.

There is also some rudimentary Mnesia stuff that isn't hashed out all that well
yet, but go wild.


## Clean-up

You can clean up the lab by running the following command:

* `task down`


## Running in Nomad

There is an `app.nomad` jobspec that you can use to run the application in
Nomad. You need to specify the Docker image to use when you run the job and that
might look like this:

```shell
nomad job run \
  -var="docker-image=<your_docker_image>" \
  app.nomad
```
