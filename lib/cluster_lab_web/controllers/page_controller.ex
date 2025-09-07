defmodule ClusterLabWeb.PageController do
  use ClusterLabWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
