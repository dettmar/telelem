defmodule Telelem.Sandbox.ResolveTest do
  use ExUnit.Case, async: true

  alias Telelem.Sandbox
  alias Telelem.Sandbox.{Bindings, Bootstrap, Principal, VerifiedTarget}

  @all Principal.permissions()

  setup do
    %{cert: der} = :public_key.pkix_test_root_cert(~c"telelem sandbox resolve CA", [])
    pem = :public_key.pem_encode([{:Certificate, der, :not_encrypted}])

    doc = %{
      "version" => 1,
      "mode" => "sandbox",
      "fixture_targets" => %{
        "a" => %{"host" => "127.0.0.1", "port" => 18443, "ca_pem" => pem},
        "b" => %{"host" => "127.0.0.1", "port" => 18444, "ca_pem" => pem}
      },
      "sandbox_store_root" => "/sbx/store",
      "sandbox_signer_ref" => "sandbox:local-fixture"
    }

    {:ok, b} = Bootstrap.from_map(doc, fixture_root: "/fx", sandbox_root: "/sbx")
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

  test "missing binding and malformed IDs fail closed", %{b: b, op: op} do
    assert {:denied, :no_binding} = Sandbox.resolve(op, b, "c")
    assert {:denied, :invalid_deployment_id} = Sandbox.resolve(op, b, "")
    assert {:denied, :invalid_deployment_id} = Sandbox.resolve(op, b, "a/../b")
    assert {:denied, :invalid_deployment_id} = Sandbox.resolve(op, b, :a)
    assert {:denied, :invalid_deployment_id} = Sandbox.resolve(op, b, String.duplicate("a", 65))
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
  end

  test "permission and principal shape are re-checked", %{b: b, op: op} do
    {:ok, reader} = Principal.new("local", "reader", :sandbox, [:receipt_read])
    assert {:denied, :permission_denied} = Sandbox.resolve(reader, b, "a")

    assert {:denied, :invalid_principal} =
             Sandbox.resolve(%{op | tenant_id: "bad tenant"}, b, "a")

    assert {:denied, :invalid_principal} =
             Sandbox.resolve(%{op | permissions: MapSet.new([:root])}, b, "a")

    assert {:denied, :invalid_principal} = Sandbox.resolve(%{tenant_id: "local"}, b, "a")
    assert {:error, :invalid_principal} = Principal.new("local", "op", :admin, [])
  end

  test "a tampered bound target is denied, not trusted", %{b: %Bindings{} = b, op: op} do
    a = b.fixture_targets["a"]
    tamper = fn t -> %Bindings{b | fixture_targets: Map.put(b.fixture_targets, "a", t)} end

    assert {:denied, :target_unverified} =
             Sandbox.resolve(op, tamper.(%{a | host: "192.0.2.1", address: {192, 0, 2, 1}}), "a")

    assert {:denied, :target_unverified} =
             Sandbox.resolve(op, tamper.(%{a | address: {127, 0, 0, 2}}), "a")

    assert {:denied, :target_unverified} =
             Sandbox.resolve(op, tamper.(%{a | deployment_id: "b"}), "a")

    assert {:denied, :target_unverified} = Sandbox.resolve(op, tamper.(%{a | cacerts: []}), "a")

    assert {:denied, :target_unverified} =
             Sandbox.resolve(op, tamper.(%{a | ca_sha256: ["00"]}), "a")

    assert {:denied, :target_unverified} = Sandbox.resolve(op, tamper.(%{a | scheme: :http}), "a")
    assert {:denied, :target_unverified} = Sandbox.resolve(op, tamper.(%{host: "127.0.0.1"}), "a")
  end
end
