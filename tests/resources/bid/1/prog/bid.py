def bida(graf):
    for w in graf:
        if len(w) % 2 != 0:
            return "NIE"
    return "TAK"


def main():
    n, m, s = map(int, input().split())
    graf = [[] for _ in range(n + 1)]
    for _ in range(m):
        a, b = map(int, input().split())
        graf[a].append(b)
        graf[b].append(a)

    print(bida(graf))


if __name__ == '__main__':
    main()
