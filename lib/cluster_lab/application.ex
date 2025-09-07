defmodule ClusterLab.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      ClusterLabWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:cluster_lab, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: ClusterLab.PubSub},
      # Start a worker by calling: ClusterLab.Worker.start_link(arg)
      # {ClusterLab.Worker, arg},
      # Start to serve requests, typically the last entry
      ClusterLabWeb.Endpoint
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: ClusterLab.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    ClusterLabWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
