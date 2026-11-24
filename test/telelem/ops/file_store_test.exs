defmodule Telelem.Ops.FileStoreTest do
  @moduledoc """
  `Telelem.Ops.FileStore` against a real directory in the runner.

  These tests run inside one VM and do not take the launcher's cross-VM lock;
  they check the store's own behaviour under that assumed contract. A "new
  VM" after an abandoned store is simulated by copying the directory to a
  fresh path (the in-VM guard is keyed by directory identity). Not covered:
  foreign file ownership (needs root), power loss, a real launcher lock.
  """
  use ExUnit.Case, async: true

  import Bitwise

  alias Telelem.Ops.{FileStore, PlanDigest, Route, Context}
  alias Telelem.Route.Revision

  @moduletag :tmp_dir

  @op %{
    tenant_id: "local",
    subject_id: "operator",
    mode: :sandbox,
    permissions: MapSet.new([:route_read, :route_write])
  }

  setup %{tmp_dir: tmp} do
    dir = Path.join(tmp, "store")
    File.mkdir!(dir)
    File.chmod!(dir, 0o700)
    %{dir: dir, tmp: tmp, rev1: rev1()}
  end

  defp rev1(attrs \\ []) do
    Revision.new!(
      Keyword.merge(
        [
          route_id: "chat",
          rev: 1,
          deployments: ["a", "b"],
          preferred: "a",
          fallback_on_refusal: true,
          deadline_ms: 5000,
          max_attempts: 2
        ],
        attrs
      )
    )
  end

  defp main(dir), do: Path.join(dir, FileStore.main_name())
  defp temp(dir), do: Path.join(dir, FileStore.temp_name())

  defp open(dir, extra \\ []) do
    name = :"fs_#{System.unique_integer([:positive])}"
    opts = [name: name, dir: dir, tenant_id: "local", route_id: "chat"] ++ extra

    case FileStore.start(opts) do
      {:ok, pid} ->
        on_exit(fn -> stop_quietly(pid) end)
        {:ok, name}

      other ->
        other
    end
  end

  defp open!(dir, extra \\ []) do
    {:ok, name} = open(dir, extra)
    name
  end

  defp stop_quietly(pid) do
    if Process.alive?(pid), do: FileStore.stop(pid)
  catch
    :exit, _ -> :ok
  end

  defp ctx(name), do: %Context{mode: :sandbox, store: {FileStore, name}, revisions: Revision}

  defp provisioned(%{dir: dir, rev1: rev1}) do
    :ok = FileStore.provision(dir, "local", rev1)
    :ok
  end

  defp plan_apply(name, expected, preferred) do
    {:ok, %{plan_digest: d}} = Route.plan(@op, ctx(name), "chat", expected, preferred)
    {Route.apply(@op, ctx(name), "chat", expected, preferred, d), d}
  end

  # A "new VM": same files, a directory the in-VM guard has never seen.
  defp copy_dir(src, tmp) do
    dst = Path.join(tmp, "copy_#{System.unique_integer([:positive])}")
    File.mkdir!(dst)
    File.chmod!(dst, 0o700)

    for name <- [FileStore.main_name(), FileStore.temp_name()],
        File.exists?(Path.join(src, name)) do
      File.cp!(Path.join(src, name), Path.join(dst, name))
      File.chmod!(Path.join(dst, name), 0o600)
    end

    dst
  end

  defp snapshot(dir) do
    for name <- [FileStore.main_name(), FileStore.temp_name()], into: %{} do
      path = Path.join(dir, name)

      case File.lstat(path) do
        {:ok, st} ->
          content = if st.type == :regular, do: File.read!(path), else: File.read_link(path)
          {name, {st.type, st.inode, st.links, content}}

        {:error, :enoent} ->
          {name, :absent}
      end
    end
  end

  defp rewrite(dir, tenant, %Revision{} = r, last) do
    File.write!(main(dir), FileStore.encode(tenant, r, last))
  end

  # Payload rewritten by `fun`, checksum recomputed: only schema checks remain.
  defp resum(dir, fun) do
    [payload, _sum, ""] = String.split(File.read!(main(dir)), "\n")
    payload = fun.(payload)
    sum = :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)
    File.write!(main(dir), payload <> "\n" <> sum <> "\n")
  end

  defp rev2(rev1, preferred) do
    {:ok, r2} = Revision.with_preferred(rev1, preferred)
    {r2, PlanDigest.compute("local", "chat", 1, preferred, r2.digest)}
  end

  ## Restart and readback

  describe "restart and readback" do
    setup :provisioned

    test "contract example survives a restart; stale apply still conflicts", %{dir: dir} do
      name = open!(dir)

      {{:ok, %{effective_revision: %Revision{rev: 2, preferred: "b"} = r2}}, d} =
        plan_apply(name, 1, "b")

      :ok = FileStore.stop(name)

      name = open!(dir)

      assert {:ok, %{effective_revision: ^r2, last_plan_digest: ^d}} =
               Route.read(@op, ctx(name), "chat")

      assert {:conflict, %{reason: :stale_revision, current_rev: 2, last_plan_digest: ^d}} =
               Route.apply(@op, ctx(name), "chat", 1, "b", d)

      # Same preferred advances rev, and that too is durable.
      {{:ok, %{effective_revision: %Revision{rev: 3, preferred: "b"} = r3}}, d3} =
        plan_apply(name, 2, "b")

      :ok = FileStore.stop(name)
      name = open!(dir)

      assert {:ok, %{revision: ^r3, last_plan_digest: ^d3}} =
               FileStore.fetch(name, "local", "chat")

      assert snapshot(dir)[FileStore.temp_name()] == :absent
    end

    test "on-disk bytes are the canonical record", %{dir: dir, rev1: rev1} do
      assert File.read!(main(dir)) == FileStore.encode("local", rev1, nil)
      [payload, sum, ""] = String.split(File.read!(main(dir)), "\n")
      assert sum == :crypto.hash(:sha256, payload) |> Base.encode16(case: :lower)

      assert payload ==
               ~s(["telelem.ops.file_store/1","local","chat",["chat",1,["a","b"],"a",true,5000,2,"#{rev1.digest}"],null])

      assert {:ok, st} = File.lstat(main(dir))
      assert (st.mode &&& 0o077) == 0
      assert byte_size(File.read!(main(dir))) <= FileStore.max_bytes()
    end

    test "tenant and route are bound; others look absent", %{dir: dir, rev1: rev1} do
      name = open!(dir)
      assert :not_found = FileStore.fetch(name, "other", "chat")
      assert :not_found = FileStore.fetch(name, "local", "other")
      {r2, d} = rev2(rev1, "b")
      expected = %{rev: 1, digest: rev1.digest}

      assert {:conflict, :not_found} =
               FileStore.compare_and_write(name, "other", "chat", expected, r2, d)

      assert :not_found = Route.read(%{@op | tenant_id: "other"}, ctx(name), "chat")
      assert {:ok, %{revision: ^rev1}} = FileStore.fetch(name, "local", "chat")
    end
  end

  ## Provisioning

  describe "provisioning" do
    test "never overwrites an existing main", %{dir: dir, rev1: rev1} do
      :ok = FileStore.provision(dir, "local", rev1)
      before = snapshot(dir)

      assert {:error, :already_provisioned} =
               FileStore.provision(dir, "local", rev1(preferred: "b"))

      assert snapshot(dir) == before
    end

    test "interrupted provision is never promoted, reseeded or cleaned", %{dir: dir, rev1: rev1} do
      File.write!(temp(dir), FileStore.encode("local", rev1, nil))
      before = snapshot(dir)
      assert {:error, :interrupted_provision} = FileStore.provision(dir, "local", rev1)
      assert {:error, :interrupted_provision} = open(dir)
      assert snapshot(dir) == before
    end

    test "unprovisioned directory is never seeded by open", %{dir: dir} do
      assert {:error, :not_provisioned} = open(dir)
      assert File.ls!(dir) == []
    end

    test "only a valid rev 1 revision", %{dir: dir, rev1: rev1} do
      {r2, _} = rev2(rev1, "b")
      assert {:error, {:invalid_revision, :rev}} = FileStore.provision(dir, "local", r2)

      assert {:error, {:invalid_revision, :digest}} =
               FileStore.provision(dir, "local", %{rev1 | deadline_ms: 1})

      assert {:error, {:invalid_revision, :revision}} =
               FileStore.provision(dir, "local", Map.from_struct(rev1))

      assert {:error, {:invalid, :tenant_id}} = FileStore.provision(dir, "bad tenant", rev1)
      assert File.ls!(dir) == []
    end

    test "directory must be a safe, normalized absolute path", %{dir: dir, tmp: tmp, rev1: rev1} do
      assert {:error, {:dir, :path}} = FileStore.provision("relative/dir", "local", rev1)
      assert {:error, {:dir, :path}} = FileStore.provision(dir <> "/../store", "local", rev1)

      link = Path.join(tmp, "link")
      File.ln_s!(dir, link)
      assert {:error, {:dir, :symlink}} = FileStore.provision(link, "local", rev1)

      File.chmod!(dir, 0o777)
      assert {:error, {:dir, :world_writable}} = FileStore.provision(dir, "local", rev1)
      assert {:error, {:dir, :world_writable}} = open(dir)
      File.chmod!(dir, 0o700)
      assert File.ls!(dir) == []
    end

    test "killed before rename: temp only, needs a fresh directory", c do
      run_killed_provision(c, :provision_synced)
      assert %{"route.rec" => :absent, "route.rec.tmp" => {:regular, _, 1, _}} = snapshot(c.dir)
      before = snapshot(c.dir)
      assert {:error, :interrupted_provision} = open(c.dir)
      assert {:error, :interrupted_provision} = FileStore.provision(c.dir, "local", c.rev1)
      assert snapshot(c.dir) == before

      fresh = Path.join(c.tmp, "fresh")
      File.mkdir!(fresh)
      File.chmod!(fresh, 0o700)
      assert :ok = FileStore.provision(fresh, "local", c.rev1)
      assert {:ok, _} = open(fresh)
    end

    test "killed after rename: provisioned", c do
      run_killed_provision(c, :provision_renamed)
      assert %{"route.rec.tmp" => :absent, "route.rec" => {:regular, _, 1, _}} = snapshot(c.dir)
      name = open!(c.dir)
      rev1 = c.rev1

      assert {:ok, %{revision: ^rev1, last_plan_digest: nil}} =
               FileStore.fetch(name, "local", "chat")
    end

    test "concurrent provisions: one wins, main is valid", %{dir: dir} do
      results =
        for p <- ["a", "b", "a", "b", "a", "b", "a", "b"] do
          Task.async(fn -> FileStore.provision(dir, "local", rev1(preferred: p)) end)
        end
        |> Task.await_many()

      assert Enum.count(results, &(&1 == :ok)) == 1

      for r <- results, r != :ok do
        assert r in [
                 {:error, :already_provisioned},
                 {:error, :interrupted_provision},
                 {:error, {:io, :create, :eexist}}
               ]
      end

      name = open!(dir)
      assert {:ok, %{revision: %Revision{rev: 1}}} = FileStore.fetch(name, "local", "chat")
      assert snapshot(dir)[FileStore.temp_name()] == :absent
    end
  end

  defp run_killed_provision(c, stage) do
    {pid, ref} =
      spawn_monitor(fn ->
        FileStore.provision(c.dir, "local", c.rev1,
          barrier: fn
            ^stage -> Process.exit(self(), :kill)
            _ -> :ok
          end
        )
      end)

    assert_receive {:DOWN, ^ref, :process, ^pid, :killed}, 5000
  end

  ## Open validation

  describe "open refuses unsafe or invalid files and leaves them untouched" do
    setup :provisioned

    test "unsafe main", %{dir: dir, tmp: tmp} do
      good = File.read!(main(dir))

      cases = [
        {fn -> File.write!(main(dir), String.replace(good, "true", "fals")) end,
         {:main, :checksum}},
        {fn -> File.write!(main(dir), binary_part(good, 0, 40)) end, {:main, :framing}},
        {fn -> File.write!(main(dir), "") end, {:main, :empty}},
        {fn -> File.write!(main(dir), :binary.copy("x", FileStore.max_bytes() + 1)) end,
         {:main, :too_large}},
        {fn -> File.chmod!(main(dir), 0o666) end, {:main, :writable_by_others}},
        {fn ->
           File.write!(Path.join(tmp, "target"), good)
           File.rm!(main(dir))
           File.ln_s!(Path.join(tmp, "target"), main(dir))
         end, {:main, :symlink}},
        {fn -> File.ln!(main(dir), Path.join(tmp, "second_link")) end, {:main, :hard_linked}},
        {fn ->
           File.rm!(main(dir))
           File.mkdir!(main(dir))
         end, {:main, :not_a_file}}
      ]

      for {tamper, reason} <- cases do
        reset(dir, tmp, good)
        tamper.()
        before = snapshot(dir)
        assert {:error, ^reason} = open(dir)
        assert snapshot(dir) == before
      end
    end

    test "checksummed but semantically invalid records", %{dir: dir, tmp: tmp, rev1: rev1} do
      good = File.read!(main(dir))
      {r2, d} = rev2(rev1, "b")

      cases = [
        {fn -> rewrite(dir, "other", rev1, nil) end, {:main, :wrong_tenant}},
        {fn -> rewrite(dir, "local", rev1(route_id: "other"), nil) end, {:main, :wrong_route}},
        {fn -> resum(dir, &String.replace(&1, ~s(["chat",1), ~s(["other",1))) end,
         {:main, :wrong_route}},
        {fn -> resum(dir, &String.replace(&1, "5000", "5001")) end, {:main, :revision}},
        {fn -> resum(dir, &String.replace(&1, ~s(["a","b"]), ~s(["b","a"]))) end,
         {:main, :revision}},
        {fn -> resum(dir, &String.replace(&1, "file_store/1", "file_store/2")) end,
         {:main, :version}},
        {fn -> resum(dir, &String.replace(&1, ",null]", "]")) end, {:main, :schema}},
        {fn -> resum(dir, &String.replace(&1, "5000", "5000.0")) end, {:main, :revision}},
        {fn -> resum(dir, &String.replace(&1, ~s("local"), ~s("\\u006cocal"))) end,
         {:main, :encoding}},
        {fn -> resum(dir, &String.replace(&1, ",", ", ")) end, {:main, :encoding}},
        {fn -> resum(dir, &(&1 <> " ")) end, {:main, :encoding}},
        {fn -> rewrite(dir, "local", rev1, d) end, {:main, :plan_digest}},
        {fn -> rewrite(dir, "local", r2, nil) end, {:main, :plan_digest}},
        {fn -> rewrite(dir, "local", r2, String.duplicate("0", 64)) end, {:main, :plan_digest}}
      ]

      for {tamper, reason} <- cases do
        reset(dir, tmp, good)
        tamper.()
        before = snapshot(dir)
        assert {:error, ^reason} = open(dir)
        assert snapshot(dir) == before
      end

      # Positive control: a correctly bound rev 2 record opens.
      reset(dir, tmp, good)
      rewrite(dir, "local", r2, d)
      assert {:ok, _} = open(dir)
    end

    test "unsafe temp is never touched, even with a valid main", %{dir: dir, tmp: tmp} do
      good = File.read!(main(dir))
      File.write!(Path.join(tmp, "victim"), "victim")

      cases = [
        {fn -> File.ln_s!(Path.join(tmp, "victim"), temp(dir)) end, {:temp, :symlink}},
        {fn -> File.ln!(Path.join(tmp, "victim"), temp(dir)) end, {:temp, :hard_linked}},
        {fn -> File.mkdir!(temp(dir)) end, {:temp, :not_a_file}},
        {fn ->
           File.write!(temp(dir), "x")
           File.chmod!(temp(dir), 0o622)
         end, {:temp, :writable_by_others}}
      ]

      for {plant, reason} <- cases do
        reset(dir, tmp, good)
        plant.()
        before = snapshot(dir)
        assert {:error, ^reason} = open(dir)
        assert snapshot(dir) == before
        assert File.read!(Path.join(tmp, "victim")) == "victim"
      end
    end

    test "owned single-link temp is removed only after main validated", %{dir: dir, tmp: tmp} do
      good = File.read!(main(dir))
      File.write!(temp(dir), "leftover")

      File.write!(main(dir), "corrupt")
      assert {:error, {:main, :framing}} = open(dir)
      assert File.read!(temp(dir)) == "leftover"

      reset(dir, tmp, good)
      File.write!(temp(dir), "leftover")
      assert {:ok, _} = open(dir)
      assert snapshot(dir)[FileStore.temp_name()] == :absent
      assert File.read!(main(dir)) == good
    end
  end

  defp reset(dir, tmp, good) do
    for path <- [main(dir), temp(dir)], do: File.rm_rf!(path)
    for name <- ["target", "second_link"], do: File.rm_rf!(Path.join(tmp, name))
    File.write!(main(dir), good)
    File.chmod!(main(dir), 0o600)
  end

  ## Compare-and-write

  describe "compare-and-write" do
    setup :provisioned

    test "refused successors write nothing", %{dir: dir, rev1: rev1} do
      name = open!(dir)
      before = snapshot(dir)
      {r2, d} = rev2(rev1, "b")
      expected = %{rev: 1, digest: rev1.digest}
      cas = &FileStore.compare_and_write(name, "local", "chat", &1, &2, &3)

      reordered =
        Revision.new!(%{Map.from_struct(r2) | deployments: ["b", "a"]} |> Map.delete(:digest))

      for {exp, new, pd, reason} <- [
            {%{expected | digest: r2.digest}, r2, d, :expected_not_current},
            {%{expected | rev: 2}, r2, d, :expected_not_current},
            {%{rev: 1}, r2, d, :expected_not_current},
            {expected, %{r2 | deadline_ms: 1}, d, :digest},
            {expected, reordered, d, :deployments},
            {expected, rev1(rev: 3, preferred: "b"), d, :rev},
            {expected, rev1(rev: 1, preferred: "b"), d, :rev},
            {expected, rev1(rev: 2, preferred: "b", max_attempts: 1), d, :max_attempts},
            {expected, r2, String.duplicate("0", 64), :plan_digest},
            {expected, r2, "short", :plan_digest},
            {expected, Map.put(Map.from_struct(r2), :extra, 1), d, {:invalid, :extra}},
            {expected, "nonsense", d, :revision}
          ] do
        assert {:error, {:rejected, ^reason}} = cas.(exp, new, pd)
      end

      assert snapshot(dir) == before

      assert {:ok, %{revision: ^rev1, last_plan_digest: nil}} =
               FileStore.fetch(name, "local", "chat")

      # A plain-map successor is accepted; a stale expected is a clean conflict.
      assert {:ok, %{revision: ^r2, last_plan_digest: ^d}} =
               cas.(expected, Map.from_struct(r2), d)

      {r3, _} = rev2(rev1, "a")
      assert {:conflict, %{revision: ^r2, last_plan_digest: ^d}} = cas.(expected, r3, d)
    end

    test "blocked before rename: disk holds old main and new temp; fetch waits", %{dir: dir} do
      test = self()

      barrier = fn stage ->
        send(test, {:at, stage, self()})
        receive do: (:go -> :ok)
      end

      name = open!(dir, barrier: barrier)
      old = File.read!(main(dir))
      {:ok, %{plan_digest: d}} = Route.plan(@op, ctx(name), "chat", 1, "b")
      apply = Task.async(fn -> Route.apply(@op, ctx(name), "chat", 1, "b", d) end)

      assert_receive {:at, :cas_synced, store}, 5000
      assert File.read!(main(dir)) == old
      assert {:ok, new} = File.read(temp(dir))
      assert {:ok, %{revision: %Revision{rev: 2}}} = FileStore.decode(new, "local", "chat")
      fetch = Task.async(fn -> FileStore.fetch(name, "local", "chat") end)
      refute Task.yield(fetch, 100)

      send(store, :go)
      assert_receive {:at, :cas_renamed, ^store}, 5000
      # Renamed, but neither state nor reply updated yet.
      assert File.read!(main(dir)) == new
      assert snapshot(dir)[FileStore.temp_name()] == :absent
      refute Task.yield(apply, 100)
      refute Task.yield(fetch, 0)

      send(store, :go)
      assert {:ok, %{effective_revision: %Revision{rev: 2}, plan_digest: ^d}} = Task.await(apply)
      assert {:ok, %{revision: %Revision{rev: 2}, last_plan_digest: ^d}} = Task.await(fetch)
    end

    test "killed before rename: outcome unknown; reopen sees old rev and drops temp", c do
      {name, d} = kill_during_apply(c, :cas_synced)
      assert %{"route.rec.tmp" => {:regular, _, 1, _}} = snapshot(c.dir)
      assert {:error, :abandoned_in_this_vm} = open(c.dir)

      fresh = copy_dir(c.dir, c.tmp)
      name2 = open!(fresh)
      assert snapshot(fresh)[FileStore.temp_name()] == :absent
      # Readback: the plan is not current (and here never landed).
      assert {:ok, %{effective_revision: %Revision{rev: 1}, last_plan_digest: nil}} =
               Route.read(@op, ctx(name2), "chat")

      refute Process.whereis(name)
      assert is_binary(d)
    end

    test "killed after rename: outcome unknown; readback confirms the plan", c do
      {_name, d} = kill_during_apply(c, :cas_renamed)
      assert snapshot(c.dir)[FileStore.temp_name()] == :absent
      assert {:error, :abandoned_in_this_vm} = open(c.dir)

      name2 = open!(copy_dir(c.dir, c.tmp))

      assert {:ok, %{effective_revision: %Revision{rev: 2, preferred: "b"}, last_plan_digest: ^d}} =
               Route.read(@op, ctx(name2), "chat")
    end

    @tag :capture_log
    test "in-process crash at a barrier is quiescent: same directory reopens", c do
      name =
        open!(c.dir,
          barrier: fn
            :cas_synced -> raise "boom"
            _ -> :ok
          end
        )

      {:ok, %{plan_digest: d}} = Route.plan(@op, ctx(name), "chat", 1, "b")
      ref = Process.monitor(Process.whereis(name))

      assert {:error, {:apply_outcome_unknown, %{cause: :store_unavailable, plan_digest: ^d}}} =
               Route.apply(@op, ctx(name), "chat", 1, "b", d)

      assert_receive {:DOWN, ^ref, :process, _, _}, 5000
      assert %{"route.rec.tmp" => {:regular, _, 1, _}} = snapshot(c.dir)
      name2 = open!(c.dir)
      assert snapshot(c.dir)[FileStore.temp_name()] == :absent
      assert {:ok, %{revision: %Revision{rev: 1}}} = FileStore.fetch(name2, "local", "chat")
    end

    test "write failure stops the store without reply and without retry", %{dir: dir} do
      name = open!(dir)
      pid = Process.whereis(name)
      ref = Process.monitor(pid)
      {:ok, %{plan_digest: d}} = Route.plan(@op, ctx(name), "chat", 1, "b")
      File.chmod!(dir, 0o500)

      try do
        assert {:error, {:apply_outcome_unknown, %{cause: :store_unavailable}}} =
                 Route.apply(@op, ctx(name), "chat", 1, "b", d)

        assert_receive {:DOWN, ^ref, :process, ^pid,
                        {:shutdown, {:write_failed, {:io, :create, :eacces}}}},
                       5000

        # Nothing retries against the stopped store.
        assert {:error, :store_unavailable} = Route.apply(@op, ctx(name), "chat", 1, "b", d)
      after
        File.chmod!(dir, 0o700)
      end

      # Clean stop path ran terminate: the same directory reopens at rev 1.
      name2 = open!(dir)
      assert {:ok, %{revision: %Revision{rev: 1}}} = FileStore.fetch(name2, "local", "chat")
    end

    test "concurrent applies: one winner per rev, durable final state", %{dir: dir, tmp: tmp} do
      name = open!(dir)

      # All plans against rev 1 first, then 16 racing applies.
      plans =
        for p <- ["a", "b"], into: %{} do
          {:ok, %{plan_digest: d}} = Route.plan(@op, ctx(name), "chat", 1, p)
          {p, d}
        end

      results =
        for p <- List.duplicate(["a", "b"], 8) |> List.flatten() do
          Task.async(fn -> Route.apply(@op, ctx(name), "chat", 1, p, plans[p]) end)
        end
        |> Task.await_many(10_000)

      assert [{:ok, %{effective_revision: %Revision{rev: 2}}}] =
               Enum.filter(results, &match?({:ok, _}, &1))

      assert Enum.count(results, &match?({:conflict, %{reason: :stale_revision}}, &1)) == 15

      # Each worker retries from a fresh read until it lands once.
      workers =
        for i <- 1..10 do
          Task.async(fn -> apply_until_landed(name, Enum.at(["a", "b"], rem(i, 2))) end)
        end

      digests = Task.await_many(workers, 30_000)
      assert length(Enum.uniq(digests)) == 10

      {:ok, %{revision: %Revision{rev: 12}} = final} = FileStore.fetch(name, "local", "chat")
      assert final.last_plan_digest in digests
      :ok = FileStore.stop(name)

      for d <- [dir, copy_dir(dir, tmp)] do
        n = open!(d)
        assert {:ok, ^final} = FileStore.fetch(n, "local", "chat")
        :ok = FileStore.stop(n)
      end
    end

    test "one live store per directory in a VM", %{dir: dir} do
      name = open!(dir)
      assert {:error, :already_open} = open(dir)

      assert {:error, {:already_started, _}} =
               FileStore.start(name: name, dir: dir, tenant_id: "local", route_id: "chat")

      assert {:error, {:invalid, :name}} =
               FileStore.start(dir: dir, tenant_id: "local", route_id: "chat")

      :ok = FileStore.stop(name)
      assert {:ok, _} = open(dir)
    end
  end

  defp apply_until_landed(name, preferred) do
    {:ok, %{effective_revision: %{rev: rev}}} = Route.read(@op, ctx(name), "chat")

    # Plan and apply are separate steps; either may see a newer revision.
    with {:ok, %{plan_digest: d}} <- Route.plan(@op, ctx(name), "chat", rev, preferred),
         {:ok, %{plan_digest: ^d}} <- Route.apply(@op, ctx(name), "chat", rev, preferred, d) do
      d
    else
      {:conflict, %{reason: :stale_revision}} -> apply_until_landed(name, preferred)
    end
  end

  defp kill_during_apply(c, stage) do
    name =
      open!(c.dir,
        barrier: fn
          ^stage -> Process.exit(self(), :kill)
          _ -> :ok
        end
      )

    {:ok, %{plan_digest: d}} = Route.plan(@op, ctx(name), "chat", 1, "b")

    assert {:error, {:apply_outcome_unknown, %{cause: :store_unavailable, plan_digest: ^d}}} =
             Route.apply(@op, ctx(name), "chat", 1, "b", d)

    {name, d}
  end
end
