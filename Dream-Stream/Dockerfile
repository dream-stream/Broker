FROM dreamstream/dotnetcore:3.0.0-bullseye-slim AS base
WORKDIR /app
EXPOSE 80
EXPOSE 443

FROM mcr.microsoft.com/dotnet/core/sdk:3.0-alpine AS build
WORKDIR /src
COPY ["Dream-Stream/Dream-Stream.csproj", "Dream-Stream/"]
RUN dotnet restore -r linux-arm "Dream-Stream/Dream-Stream.csproj"
COPY . .
WORKDIR "/src/Dream-Stream"
RUN dotnet build "Dream-Stream.csproj" -r linux-arm -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Dream-Stream.csproj" -r linux-arm -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
COPY libgrpc_csharp_ext.x86.so libgrpc_csharp_ext.x86.so
ENTRYPOINT ["dotnet", "Dream-Stream.dll"]