defmodule Telelem.Sandbox.ResolveTest do
  use ExUnit.Case, async: true

  alias Telelem.Sandbox
  alias Telelem.Sandbox.{Bindings, Bootstrap, Principal, VerifiedTarget}

  @moduletag :tmp_dir
  @all Principal.permissions()

  setup %{tmp_dir: d} do
    %{cert: der} = :public_key.pkix_test_root_cert(~c"telelem sandbox resolve CA", [])
    pem = :public_key.pem_encode([{:Certificate, der, :not_encrypted}])
    fx = Path.join(d, "fx")
    sb = Path.join(d, "sbx")
    File.mkdir_p!(fx)
    File.mkdir_p!(Path.join(sb, "store"))

    doc = %{
      "version" => 2,
      "tenant_id" => "local",
      "mode" => "sandbox",
      "fixture_targets" => %{
        "a" => %{"host" => "127.0.0.1", "port" => 18443, "ca_pem" => pem},
        "b" => %{"host" => "127.0.0.1", "port" => 18444, "ca_pem" => pem}
      },
      "sandbox_store_root" => Path.join(sb, "store"),
      "sandbox_signer_ref" => "sandbox:local-fixture"
    }

    {:ok, b} = Bootstrap.from_map(doc, fixture_root: fx, sandbox_root: sb)
    {:ok, op} = Principal.new("local", "operator", :sandbox, @all)
    %{b: b, op: op, der: der}
  end

  test "contract example: local/operator/sandbox resolves a to its fixture target and CA", %{
    b: b,
    op: op,
    der: der
  } do
    assert {:ok,
            %VerifiedTarget{deployment_id: "a", host: "127.0.0.1", port: 18443, cacerts: [^der]}} =
             Sandbox.resolve(op, b, "a")

    assert {:ok, %VerifiedTarget{deployment_id: "b", port: 18444}} = Sandbox.resolve(op, b, "b")
  end

  test "verifier enrollment facts are exact: IP literal, port, CA digest", %{
    b: b,
    op: op,
    der: der
  } do
    {:ok, t} = Sandbox.resolve(op, b, "a")
    assert t.address == {127, 0, 0, 1}
    assert List.to_string(:inet.ntoa(t.address)) == t.host
    assert t.ca_sha256 == [Base.encode16(:crypto.hash(:sha256, der), case: :lower)]

    assert VerifiedTarget.tls_opts(t) == [
             cacerts: [der],
             verify: :verify_peer,
             versions: [:"tlsv1.3", :"tlsv1.2"]
           ]
  end

  test "missing binding and malformed IDs fail closed", %{b: b, op: op} do
    assert {:denied, :no_binding} = Sandbox.resolve(op, b, "c")
    assert {:denied, :invalid_deployment_id} = Sandbox.resolve(op, b, "")
    assert {:denied, :invalid_deployment_id} = Sandbox.resolve(op, b, "a/../b")
    assert {:denied, :invalid_deployment_id} = Sandbox.resolve(op, b, :a)
    assert {:denied, :invalid_deployment_id} = Sandbox.resolve(op, b, String.duplicate("a", 65))
  end

  test "bindings are tenant-scoped; a foreign binding looks missing", %{
    b: %Bindings{} = b,
    op: op
  } do
    {:ok, other} = Principal.new("other", "operator", :sandbox, @all)

    for id <- ["a", "b", "c"] do
      assert {:denied, :no_binding} = Sandbox.resolve(other, b, id)
    end

    # Same subject and permissions, only tenant differs: the outcome for a
    # bound ID equals the outcome for an unbound one.
    assert Sandbox.resolve(other, b, "a") == Sandbox.resolve(op, b, "c")

    # Tenant is taken from the principal only; rewriting the bindings' tenant
    # to the caller's is a different, separately bootstrapped envelope.
    assert {:ok, _} = Sandbox.resolve(other, %Bindings{b | tenant_id: "other"}, "a")
    assert {:denied, :no_binding} = Sandbox.resolve(op, %Bindings{b | tenant_id: "other"}, "a")

    assert {:denied, :invalid_bindings} =
             Sandbox.resolve(op, %Bindings{b | tenant_id: "bad tenant"}, "a")

    assert {:denied, :invalid_bindings} = Sandbox.resolve(op, %Bindings{b | tenant_id: nil}, "a")
  end

  test "serving principals have no resolver path in this slice", %{b: b} do
    {:ok, serving} = Principal.new("local", "operator", :serving, @all)
    assert {:denied, :no_serving_resolver} = Sandbox.resolve(serving, b, "a")
  end

  test "mismatched or absent bindings fail closed", %{b: %Bindings{} = b, op: op} do
    assert {:denied, :invalid_bindings} = Sandbox.resolve(op, nil, "a")

    assert {:denied, :invalid_bindings} =
             Sandbox.resolve(op, %{fixture_targets: b.fixture_targets}, "a")

    assert {:denied, :invalid_bindings} = Sandbox.resolve(op, %Bindings{b | mode: :serving}, "a")

    assert {:denied, :invalid_bindings} =
             Sandbox.resolve(op, %Bindings{b | fixture_targets: %{}}, "a")

    assert {:denied, :invalid_bindings} =
             Sandbox.resolve(op, %Bindings{b | sandbox_signer_ref: "file:/k"}, "a")

    assert {:denied, :invalid_bindings} =
             Sandbox.resolve(
               op,
               %Bindings{b | sandbox_store_root: "/var/lib/telelem/production"},
               "a"
             )

    assert {:denied, :invalid_bindings} = Sandbox.resolve(op, Map.put(b, :resolver, :prod), "a")
  end

  test "permission and principal shape are re-checked", %{b: b, op: op} do
    {:ok, reader} = Principal.new("local", "reader", :sandbox, [:receipt_read])
    assert {:denied, :permission_denied} = Sandbox.resolve(reader, b, "a")

    {:ok, writer} = Principal.new("local", "writer", :sandbox, [:route_read, :route_write])
    assert {:denied, :permission_denied} = Sandbox.resolve(writer, b, "a")

    {:ok, dispatcher} = Principal.new("local", "gw", :sandbox, [:dispatch])
    assert {:ok, _} = Sandbox.resolve(dispatcher, b, "a")

    assert {:denied, :invalid_principal} =
             Sandbox.resolve(%{op | tenant_id: "bad tenant"}, b, "a")

    assert {:denied, :invalid_principal} =
             Sandbox.resolve(%{op | permissions: MapSet.new([:root])}, b, "a")

    assert {:denied, :invalid_principal} =
             Sandbox.resolve(%{op | permissions: [:dispatch]}, b, "a")

    assert {:denied, :invalid_principal} = Sandbox.resolve(%{tenant_id: "local"}, b, "a")
    assert {:error, :invalid_principal} = Principal.new("local", "op", :admin, [])
  end

  test "a tampered bound target is denied, not trusted", %{b: %Bindings{} = b, op: op} do
    a = b.fixture_targets["a"]
    tamper = fn t -> %Bindings{b | fixture_targets: Map.put(b.fixture_targets, "a", t)} end

    for bad <- [
          %{a | host: "192.0.2.1", address: {192, 0, 2, 1}},
          %{a | address: {127, 0, 0, 2}},
          %{a | host: "0:0:0:0:0:0:0:1", address: {0, 0, 0, 0, 0, 0, 0, 1}},
          %{a | deployment_id: "b"},
          %{a | cacerts: []},
          %{a | cacerts: [hd(a.cacerts) <> <<0>>]},
          %{a | ca_sha256: ["00"]},
          %{a | scheme: :http},
          %{a | port: "18443"},
          Map.put(a, :sni, "prod.example.invalid"),
          %{host: "127.0.0.1"}
        ] do
      t = tamper.(bad)
      assert {:denied, :invalid_bindings} = Sandbox.resolve(op, t, "a")
      # The whole envelope is rejected, so an untouched sibling is denied too.
      assert {:denied, :invalid_bindings} = Sandbox.resolve(op, t, "b")
    end

    # Two IDs bound to one endpoint.
    dup = %{b.fixture_targets["b"] | port: a.port}

    assert {:denied, :invalid_bindings} =
             Sandbox.resolve(
               op,
               %Bindings{b | fixture_targets: Map.put(b.fixture_targets, "b", dup)},
               "a"
             )

    # Map key that is not an ID.
    assert {:denied, :invalid_bindings} =
             Sandbox.resolve(
               op,
               %Bindings{b | fixture_targets: Map.put(b.fixture_targets, "bad id", a)},
               "a"
             )
  end
end
