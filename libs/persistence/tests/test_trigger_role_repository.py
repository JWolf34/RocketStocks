"""TriggerRoleRepository: which Discord role an Alert pings for each Trigger."""

from rs_persistence import TriggerRoleRepository


async def test_a_mapping_is_stored_and_read_back(session_factory):
    async with session_factory() as s:
        repo = TriggerRoleRepository(s)
        assert await repo.all() == {}
        assert await repo.get("popularity_surge") is None
        await repo.set("popularity_surge", 111)
        await s.commit()

    async with session_factory() as s:
        repo = TriggerRoleRepository(s)
        assert await repo.get("popularity_surge") == 111
        assert await repo.all() == {"popularity_surge": 111}


async def test_mapping_a_trigger_again_replaces_its_role(session_factory):
    async with session_factory() as s:
        repo = TriggerRoleRepository(s)
        await repo.set("popularity_surge", 111)
        await repo.set("popularity_surge", 222)
        await repo.set("other", 111)  # a role may serve several Triggers
        await s.commit()

    async with session_factory() as s:
        assert await TriggerRoleRepository(s).all() == {"popularity_surge": 222, "other": 111}


async def test_removing_a_mapping_says_whether_there_was_one(session_factory):
    async with session_factory() as s:
        repo = TriggerRoleRepository(s)
        await repo.set("popularity_surge", 111)
        await s.commit()

    async with session_factory() as s:
        repo = TriggerRoleRepository(s)
        assert await repo.remove("popularity_surge") is True
        assert await repo.remove("popularity_surge") is False
        await s.commit()

    async with session_factory() as s:
        assert await TriggerRoleRepository(s).all() == {}
