import discord
from discord import app_commands
from discord.ext import commands
from discord.ext import tasks

class Greetings(commands.Cog):

    def __init__(self, client):
        self.client = client

    @commands.command()
    async def hello(self, ctx, *, member:discord.Member):
        await ctx.send(f"Hello {member.name}")
    
    @app_commands.command(name = "fetch-logs", description = "Hello",)
    async def slash_hello(self, interaction:discord.Interaction):
        await interaction.response.send_message("Hello!")

async def setup(client):
    await client.add_cog(Greetings(client))