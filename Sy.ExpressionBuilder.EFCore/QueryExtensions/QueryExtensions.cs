using Microsoft.EntityFrameworkCore;
using Sy.ExpressionBuilder.Expressions;
using Sy.ExpressionBuilder.Extensions;
using Sy.ExpressionBuilder.Modules;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;


namespace Sy.ExpressionBuilder.EFCore.QueryExtensions
{
    /// <summary>
    /// 查询扩展
    /// </summary>
    public static class QueryExtensions
    {

        /// <summary>
        /// 获取条件下列表
        /// </summary>
        /// <param name="queryModel"></param>
        /// <param name="total"></param>
        /// <returns></returns>
        public static async Task<List<T>> ToListAsync<T>(this IQueryable<T> source, QueryBase input)
        {
            return await source.Where(input.ToExpression<T>()).OrderByItem(input.OrderByItems.ToArray()).ToListAsync();
        }

        /// <summary>
        /// 获取条件下列表
        /// </summary>
        /// <param name="input"></param>
        /// <param name="total"></param>
        /// <returns></returns>
        public static async Task<List<T>> ToListAsync<T>(this IQueryable<T> source, QueryModel input)
        {
            return await source.Where(input.ToExpression<T>()).OrderByItem(input.OrderByItems.ToArray()).ToListAsync();
        }

        /// <summary>
        /// 获取条件下列表
        /// </summary>
        /// <param name="input"></param>
        /// <param name="total"></param>
        /// <returns></returns>
        public static async Task<(List<T> list, int total)>PageToListAsync<T>(this IQueryable<T> source, QueryModel input)
        {
            source = source.Where(input.ToExpression<T>());
            var total = await source.CountAsync();
            var list = await source.OrderByItem(input.OrderByItems.ToArray()).ToListAsync();
            return (list, total);
        }

        /// <summary>
        /// 获取条件下列表
        /// </summary>
        /// <param name="queryModel"></param>
        /// <param name="total"></param>
        /// <returns></returns>
        public static async Task<(List<T> list, int total)> PageToListAsync<T>(this IQueryable<T> source, QueryBase input)
        {
            source = source.Where(input.ToExpression<T>());
            var total = await source.CountAsync();
            var list = await source.OrderByItem(input.OrderByItems.ToArray()).ToListAsync();
            return (list, total);
        }

        /// <summary>
        /// 获取条件下列表
        /// </summary>
        /// <param name="input"></param>
        /// <param name="total"></param>
        /// <returns></returns>
        public static async Task<(List<T> list, int total)> PageToListAsync<T>(this IQueryable<T> source, QueryPageModel input)
        {
            source = source.Where(input.ToExpression<T>());
            var total = await source.CountAsync();
            var list = await source.OrderByItem(input.OrderByItems.ToArray()).Page<T>(input).ToListAsync();
            return (list, total);
        }

        /// <summary>
        /// 获取条件下列表
        /// </summary>
        /// <param name="queryModel"></param>
        /// <param name="total"></param>
        /// <returns></returns>
        public static async Task<(List<T> list, int total)> PageToListAsync<T>(this IQueryable<T> source, PageModel input)
        {
            source = source.Where(input.ToExpression<T>());
            var total = await source.CountAsync();
            var list = await source.OrderByItem(input.OrderByItems.ToArray()).Page<T>(input).ToListAsync();
            return (list, total);
        }


        #region Include
        /// <summary>
        /// 获取包含条件下数据
        /// </summary>
        /// <param name="queryModel"></param>
        /// <param name="total"></param>
        /// <returns></returns>
        public static IQueryable<T> IncludeRange<T>(this IQueryable<T> source, params Expression<Func<T, dynamic>>[] expressions) where T : class
        {
            foreach (var expression in expressions)
            {
                source = source.Include(expression);
            }
            return source;
        }


        /// <summary>
        /// 获取包含条件下数据
        /// </summary>
        /// <param name="queryModel"></param>
        /// <param name="total"></param>
        /// <returns></returns>
        public static async Task<T> IncludeFirstOrDefaultAsync<T>(this IQueryable<T> source, params Expression<Func<T, dynamic>>[] expressions) where T : class
        {
            foreach (var expression in expressions)
            {
                source = source.Include(expression);
            }
            return await source.FirstOrDefaultAsync();
        }

        /// <summary>
        /// 获取包含条件下列表
        /// </summary>
        /// <param name="queryModel"></param>
        /// <param name="total"></param>
        /// <returns></returns>
        public static async Task<List<T>> IncludeListAsync<T>(this IQueryable<T> source, params Expression<Func<T, dynamic>>[] expressions) where T : class
        {
            foreach (var expression in expressions)
            {
                source = source.Include(expression);
            }
            return await source.ToListAsync();
        }
        #endregion


    }
}
