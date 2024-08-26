from datetime import datetime
import argparse
from utils.utils import get_gmde_stand_dates, create_snapshots


def main(start_point: str, end_point: str):
    gmde_stand_dates = get_gmde_stand_dates(start_point, end_point)
    create_snapshots(gmde_stand_dates)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-sp', '--start_point', default='01-01-1981')
    parser.add_argument('-ep', '--end_point',
                        default=str(datetime.today().strftime('%d-%m-%Y')))
    args = parser.parse_args()
    main(args.start_point, args.end_point)
