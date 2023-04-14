
## Requirements

For the unit tests to run we'll need to have both consul and localstack installed and running.
Localstack is a quite functional mock of all the aws services.
Consul is the service discovery gadget we use.

On a mac the install is:

```brew intall localstack```

and

```brew install consul```

The install does not set these up to run at reboot. We'll run them manuall

In two separate terminal windows start these two services with 

```localstack start```

and

```consul agent -dev```

Just leave them running while working on the code and running tests.

This means that CI becomes more complicated TODO: fix ci
We can use github actions.

